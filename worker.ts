#!/usr/bin/env bun
/**
 * TeslApp — Fleet Telemetry worker (Bun + TypeScript port of worker.py)
 *
 * Consumes protobuf-encoded Tesla telemetry from Kafka and writes the fields
 * we care about into the fleet_telemetry.* tables in PostgreSQL.
 *
 *   Run:    bun run worker.ts
 *   Debug:  DEBUG=1 bun run worker.ts   (decode + print, no DB writes, no offset commit)
 *
 * Requires vehicle_data.proto next to this file (the .proto you generated
 * vehicle_data_pb2.py from). protobufjs resolves google/protobuf/timestamp.proto
 * on its own.
 */

import * as protobuf from "protobufjs";
import { Kafka, logLevel } from "kafkajs";
import postgres from "postgres";

// ─── Config ──────────────────────────────────────────────────────────────────
const KAFKA_BROKER = Bun.env.KAFKA_BROKER ?? "172.22.0.3:9092";
const KAFKA_TOPIC = Bun.env.KAFKA_TOPIC ?? "tesla_telemetry_V";
const PROTO_PACKAGE = "telemetry"; // package name declared in vehicle_data.proto
const PROTO_PATH = `${import.meta.dir}/proto_files/vehicle_data.proto`;
const DEBUG = Bun.env.DEBUG === "1";

// Discrete fields on purpose: the password contains # < > ( ) [ ] : that a
// connection URI would mangle (libpq is lenient about it, most TS clients aren't).
// Prefer env vars — and rotate this, it's plaintext in a file now.
const PG = {
    host: Bun.env.PGHOST,
    port: Number(Bun.env.PGPORT),
    user: Bun.env.PGUSER,
    password: Bun.env.PGPASSWORD,
    database: Bun.env.PGDATABASE,
};

// ─── Protobuf ────────────────────────────────────────────────────────────────
const root = await protobuf.load(PROTO_PATH);
const Payload = root.lookupType(`${PROTO_PACKAGE}.Payload`);
const FieldEnum = root.lookupEnum(`${PROTO_PACKAGE}.Field`);

// id → "HvacACEnabled" etc. — the equivalent of vehicle_data_pb2.Field.Name()
const fieldName = (id: number): string | undefined => FieldEnum.valuesById[id];

// ─── Value helpers ───────────────────────────────────────────────────────────
// After toObject({ oneofs: true }) a decoded Value looks like:
//   { value: "booleanValue", booleanValue: true }
// where `value` is the name of the set oneof member — same idea as WhichOneof.
function getValue(value: any): [string | null, any] {
    const which: string | undefined = value?.value;
    if (!which) return [null, null];
    return [which, value[which]];
}

const asBool = Boolean;
const asFloat = Number;
const asInt = (v: any) => Math.trunc(Number(v));

// Python wrote naive-UTC datetimes. We emit "YYYY-MM-DD HH:MM:SS.mmm+00", which
// lands correctly in BOTH `timestamp` (offset ignored) and `timestamptz`.
function pgTimestamp(d: Date): string {
    return d.toISOString().replace("T", " ").replace("Z", "+00");
}

// Returns null when the value isn't a usable timestamp. Tesla reports some
// time fields (e.g. ScheduledChargingStartTime) as a boolean when no time is
// set, so callers should treat null as "nothing to record" and skip — never
// throw, or one such field would roll back the whole payload's transaction.
function asTimestamp(v: any): Date | null {
    if (typeof v === "boolean") return null; // no scheduled time set
    if (typeof v === "number") {
        if (!Number.isFinite(v) || v <= 0) return null;
        return new Date(v * 1000); // epoch seconds
    }
    if (typeof v === "string") {
        // interpret as UTC wall-clock, matching the Python strptime behaviour
        const iso = /^\d{4}-\d{2}-\d{2}[ T]/.test(v) ? v.replace(" ", "T") + "Z" : v;
        let d = new Date(iso);
        if (Number.isNaN(+d)) d = new Date(v + " UTC");
        if (Number.isNaN(+d)) return null;
        return d;
    }
    return null;
}

function payloadTimestamp(payload: any): Date {
    const ca = payload.createdAt; // google.protobuf.Timestamp → { seconds, nanos }
    if (ca?.seconds) return new Date(ca.seconds * 1000 + (ca.nanos ?? 0) / 1e6);
    return new Date(); // fallback if created_at isn't populated
}

// ─── Handlers ────────────────────────────────────────────────────────────────
type Handler = (sql: any, vin: string, value: any, ts: Date) => Promise<void>;
const HANDLERS: Record<string, Handler> = {};
const on = (field: string, fn: Handler) => {
    HANDLERS[field] = fn;
};

on("HvacACEnabled", async (sql, vin, v, ts) => {
    await sql`INSERT INTO fleet_telemetry.ac_enabled (vin, hvac_ac_enabled, timestamp)
              VALUES (${vin}, ${asBool(v)}, ${pgTimestamp(ts)})`;
});

on("ChargeEnableRequest", async (sql, vin, v, ts) => {
    await sql`INSERT INTO fleet_telemetry.charge_enable (vin, charge_enable_request, timestamp)
              VALUES (${vin}, ${asBool(v)}, ${pgTimestamp(ts)})`;
});

on("BatteryLevel", async (sql, vin, v, ts) => {
    await sql`INSERT INTO fleet_telemetry.charge_level (vin, battery_level, timestamp)
              VALUES (${vin}, ${asFloat(v)}, ${pgTimestamp(ts)})`;
});

on("ScheduledChargingStartTime", async (sql, vin, v, ts) => {
    const when = asTimestamp(v);
    if (when === null) return; // no scheduled charging time set — nothing to record
    await sql`INSERT INTO fleet_telemetry.charge_scheduled (vin, scheduled_charging_start_time, timestamp)
              VALUES (${vin}, ${pgTimestamp(when)}, ${pgTimestamp(ts)})`;
});

on("ClimateKeeperMode", async (sql, vin, v, ts) => {
    await sql`INSERT INTO fleet_telemetry.climate_keeper_mode (vin, climate_keeper_mode, timestamp)
              VALUES (${vin}, ${asInt(v)}, ${pgTimestamp(ts)})`;
});

on("InsideTemp", async (sql, vin, v, ts) => {
    await sql`INSERT INTO fleet_telemetry.temp_int (vin, inside_temp, timestamp)
              VALUES (${vin}, ${asFloat(v)}, ${pgTimestamp(ts)})`;
});

on("Location", async (sql, vin, v, ts) => {
    // v is the LocationValue message: { latitude, longitude }
    await sql`INSERT INTO fleet_telemetry.location (vin, latitude, longitude, timestamp)
              VALUES (${vin}, ${Number(v.latitude)}, ${Number(v.longitude)}, ${pgTimestamp(ts)})`;
});

on("DriveRail", async (sql, vin, v, ts) => {
    await sql`INSERT INTO fleet_telemetry.drive_rail (vin, drive_rail, timestamp)
              VALUES (${vin}, ${Boolean(v)}, ${pgTimestamp(ts)})`;
})

// ─── Kafka connect with retry ─────────────────────────────────────────────────
async function connectWithRetry(
    consumer: { connect(): Promise<void> },
    maxRetries = 10,
    delayMs = 3000,
): Promise<void> {
    for (let i = 1; i <= maxRetries; i++) {
        try {
            await consumer.connect();
            return;
        } catch (e) {
            const msg = e instanceof Error ? e.message : String(e);
            console.warn(`[${hms()}] [connect] attempt ${i}/${maxRetries} failed: ${msg}`);
            if (i === maxRetries) throw e;
            await Bun.sleep(delayMs);
        }
    }
}

// ─── Main loop ───────────────────────────────────────────────────────────────
const hms = (d = new Date()) => d.toLocaleTimeString("en-GB", { hour12: false });
const utc = (d: Date) => d.toISOString().slice(11, 19);

async function main() {
    const sql = DEBUG ? null : postgres({ ...PG, max: 4 });
    if (DEBUG) console.log("DEBUG mode — DB writes disabled");

    const kafka = new Kafka({
        clientId: "telemetry-worker",
        brokers: [KAFKA_BROKER],
        logLevel: logLevel.ERROR,
        requestTimeout: 30000,
        connectionTimeout: 10000,
    });
    const consumer = kafka.consumer({
        groupId: "telemetry-worker",
        sessionTimeout: 30000,
        heartbeatInterval: 3000,
        rebalanceTimeout: 60000,
    });

    await connectWithRetry(consumer);
    await consumer.subscribe({ topic: KAFKA_TOPIC, fromBeginning: false }); // = auto_offset_reset latest

    console.log(`[${hms()}] Worker started`);

    const shutdown = async () => {
        try {
            await consumer.disconnect();
        } catch {}
        try {
            await sql?.end({ timeout: 5 });
        } catch {}
        process.exit(0);
    };
    process.on("SIGINT", shutdown);
    process.on("SIGTERM", shutdown);

    await consumer.run({
        autoCommit: !DEBUG, // in debug, read-and-print without advancing offsets
        eachMessage: async ({ message }) => {
            if (!message.value) return; // tombstone / empty
            try {
                const decoded = Payload.decode(message.value);
                const payload: any = Payload.toObject(decoded, {
                    longs: Number,
                    enums: Number,
                    defaults: false,
                    oneofs: true,
                });

                const vin: string = payload.vin;
                const ts = payloadTimestamp(payload);
                const data: any[] = payload.data ?? [];

                if (DEBUG) {
                    console.log(`\n[${hms()}] VIN: ${vin} | payload_ts: ${utc(ts)}`);
                    for (const datum of data) {
                        const name = fieldName(datum.key) ?? `#${datum.key}`;
                        const [type, value] = getValue(datum.value);
                        console.log(`  ${name}: type=${type}, value=${JSON.stringify(value)}`);
                    }
                    return;
                }

                const inserted: string[] = [];
                await sql!.begin(async (tx) => {
                    for (const datum of data) {
                        const name = fieldName(datum.key);
                        if (!name) continue;
                        const handler = HANDLERS[name];
                        if (!handler) continue;
                        const [, value] = getValue(datum.value);
                        if (value === null || value === undefined) continue;
                        await handler(tx, vin, value, ts);
                        inserted.push(name);
                    }
                });

                console.log(`[${hms()}] ${vin} @ ${utc(ts)} — inserted: ${inserted.join(", ") || "nothing"}`);
            } catch (e) {
                // Mirror the Python: log and move on. The offset still advances, so a
                // permanently-bad message can't wedge the consumer.
                console.error(`[${hms()}] ERROR: ${e instanceof Error ? e.message : e}`);
            }
        },
    });
}

main().catch((e) => {
    console.error("fatal:", e);
    process.exit(1);
});