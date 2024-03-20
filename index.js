// npm i better-sqlite3 github:sovlet/sq-lib
const { open } = require("fs").promises;
const db = require("better-sqlite3")("./profiles.db");
const { GameClient, ClientData, Logger } = require("sq-lib");

const GAPS_MODE = true;

let mask = 0 | 4 | 8 | 16 | 128 | 256 | 1024 | 65536,
    logNet = false,
    uid,
    session =
        "ВАШ ТОКЕН";
const guardReference = Buffer.from([6, 0, 0, 0, 2, 0, 0, 0, 0, 0]);

db.prepare(
    "CREATE TABLE IF NOT EXISTS profiles (uid INTEGER PRIMARY KEY, level INTEGER, shaman_level INTEGER, exp INTEGER, shaman_exp INTEGER, name TEXT, profile TEXT, sex INTEGER, moderator INTEGER, clan_id INTEGER, bdate INTEGER)"
).run();

function terminate() {
    console.log("Завершение работы...");
    db.close();
    process.exit();
}

process.on("SIGINT", terminate);
process.on("SIGTERM", terminate);

Logger.setOptions({
    logFile: 0,
    debug: 0,
    info: 1,
    warn: 1,
    error: 1,
    fatal: 1
});
function log(...args) {
    Logger.info(
        "net",
        new Date().toLocaleTimeString("ru-RU", {
            hour12: false,
            hour: "numeric",
            minute: "numeric",
            second: "numeric"
        }),
        ...args
    );
}

const createClient = (host, ports) =>
    new GameClient({
        port: ports[Math.floor(Math.random() * ports.length)],
        host: host
    });

const client = createClient("88.212.206.137", ["11111", "11211", "11311"]);
client.on("client.connect", () => handleConnect(client));
client.on("client.close", () => handleClose(client));
client.on("packet.incoming", (packet, buffer) => handlePacket(packet, buffer));
client.on("packet.incoming", (packet, buffer) => logPacket(packet, buffer, 0));
client.on("packet.outcoming", (packet, buffer) => logPacket(packet, buffer, 1));
client.setMaxListeners(0);
client.open();

function waitForResult(emitter, event, type, timeout = 0) {
    const onPacket = (resolve, packet) => {
        if (
            (typeof type === "function" && type(packet)) ||
            packet.type === type
        ) {
            emitter.off(event, onPacket);
            clearTimeout(timeoutId);
            resolve(packet);
        }
    };
    let timeoutId;
    return new Promise((resolve, reject) => {
        emitter.on(event, onPacket.bind(this, resolve));
        if (timeout > 0) {
            timeoutId = setTimeout(() => {
                emitter.off(event, onPacket);
                console.error("Timeout", type);
                reject(new Error("Timeout"));
            }, timeout);
        }
    });
}

function executeAndWait(emitter, func, event, type, timeout = 0) {
    const promise = waitForResult(emitter, event, type, timeout);
    func();
    return promise;
}

function queryStringToObject(queryString) {
    let result = {};
    queryString.split("&").forEach(item => {
        let [key, value] = item.split("=");
        result[key] = value;
    });
    return result;
}

function composeLogin(token) {
    const session = queryStringToObject(token);
    let id,
        netType,
        OAuth,
        key,
        tag,
        ref,
        result = [];
    id = BigInt(session.userId);
    netType = parseInt(session.net_type);
    OAuth = +session.OAuth;
    switch (session.useApiType) {
        case "sa":
            key = session.authKey;
            ref = -1;
            break;
        case "ok":
            key = session.auth_sig;
            ref = 20000;
            break;
        case "vk":
            key = "";
            ref = 0;
            break;
        case "mm":
            key = "";
            ref = 10000;
    }
    result = [id, netType, OAuth, key, 3, ref];
    if (session.useApiType !== "sa") result.push(session.token);
    return result;
}

function experienceToLevel(exp) {
    const levels = ClientData.ConfigData.player.levels;
    for (let i = 0; i < levels.length; i++)
        if (exp < levels[i].experience) return i - 1;
    return ClientData.ConfigData.player.MAX_LEVEL;
}

function shamanExperienceToLevel(exp) {
    const levels = ClientData.ConfigData.shaman.levels;
    for (let i = 0; i < levels.length; i++) if (exp < levels[i]) return i + 1;
    return ClientData.ConfigData.shaman.MAX_LEVEL + 1;
}

let pingInterval;
function logPacket(packet, buffer, out) {
    if (out) {
        clearInterval(pingInterval);
        pingInterval = setInterval(() => client.sendData("PING", 0), 30000);
    }
    logNet && log(packet, JSON.stringify(buffer));
}

async function handleClose() {
    log("Сервер закрыл соединение");
    process.exit(1);
}

async function handleConnect() {
    client.sendData("HELLO");
    let login = { data: { status: 2 } };
    while (login.data.status === 2) {
        login = await executeAndWait(
            client,
            () => client.sendData("LOGIN", ...composeLogin(session)),
            "packet.incoming",
            "PacketLogin",
            1000
        );
    }
    uid = login.data.innerId;
    const start = db
        .prepare(
            "SELECT COALESCE(MAX(uid), 0) AS uid FROM profiles WHERE uid != ?"
        )
        .get(uid).uid;
    if (login.data.status !== 0) {
        log(uid, "Закрываем сокет");
        client.close();
        return;
    }
    log(uid, "статус логина:" + login.data.status);
    GAPS_MODE ? find_gaps() : seek(start + 1, PAGE_SIZE);
}

const idsgen_helper = (start, end) =>
    Array.from({ length: end - start + 1 }, (_, a) => [a + start]);

async function grab(start, paging, buffer) {
    const end = start + paging,
        ids = buffer || idsgen_helper(start, end);
    console.log(
        `Запрос следующих ${ids.length} шт. профилей, начиная с ${ids[0][0]}`
    );
    client.sendData("REQUEST", ids, mask);
}

const PAGE_SIZE = 600;
async function seek(i, paging, target) {
    while (!target || target - i >= paging) {
        grab(i, paging);
        await waitForNumericResult(i + paging);
        i += paging + 1;
    }
    grab(i, target - i);
}

const insert = db.prepare(
    "INSERT or REPLACE INTO profiles (uid, level, shaman_level, exp, shaman_exp, name, profile, sex, moderator, clan_id, bdate) VALUES (@1, @2, @3, @4, @5, @6, @7, @8, @9, @10, @11)"
);
const insertMany = db.transaction(profiles => {
    for (const profile of profiles) insert.run(profile);
});

let fire = log;
const waitForNumericResult = end =>
    new Promise(r => {
        fire = current => end - current < 20 && r();
    });

async function handlePacket(packet, buffer) {
    switch (packet.type) {
        case "PacketGuard":
            if (Buffer.compare(buffer, guardReference) !== 0) {
                log(this.self.uid, "Гард изменился, выходим.");
                process.exit();
            }
            client.sendData("GUARD", []);
            break;
        case "PacketInfo":
            const profiles = packet.data.data;
            const pendingUpdate = [];
            let lastProfile = 0;
            for (var i = 0; i < profiles.length; i++) {
                const profile = profiles[i];
                if (lastProfile < profile.uid && lastProfile != uid)
                    lastProfile = profile.uid;
                log("Получили информацию об игроке", profile.uid);
                pendingUpdate.push({
                    1: profile.uid,
                    2: experienceToLevel(profile.exp),
                    3: shamanExperienceToLevel(profile.shaman_exp),
                    4: profile.exp,
                    5: profile.shaman_exp,
                    6: profile.name,
                    7: profile.person_info.profile,
                    8: profile.sex,
                    9: profile.moderator,
                    10: profile.clan_id,
                    11: profile.person_info.bdate
                });
            }
            insertMany(pendingUpdate);
            fire(lastProfile);
    }
}

async function find_gaps() {
    log("Ищем пропуски...");
    const f = await open("inconsistent.txt", "a");
    await f.truncate();
    const uids = db
        .prepare("SELECT uid FROM profiles ORDER BY uid")
        .all()
        .map(x => x.uid);
    let start = uids[0];
    for (let i = 1; i < uids.length; i++) {
        if (uids[i] !== uids[i - 1] + 1)
            f.appendFile(`${start + 1}-${uids[i] - 1}\n`).catch(log);
        start = uids[i];
    }
    f.close().then(walk_gaps);
}

async function walk_gaps() {
    console.log("Запущено заполнение пустот 🤤");
    const f = await open("inconsistent.txt", "r");
    let start, end;
    let ids = [];
    for await (const line of f.readLines()) {
        log("В обработке:", line);
        const range = line.split("-").map(x => parseInt(x));
        ids = [...ids, ...idsgen_helper(...range)];
        if (ids.length > PAGE_SIZE) grab(null, null, ids.splice(0, PAGE_SIZE));
    }
    grab(null, null, ids);
    f.close();
}

(async () => {
    const repl = require("repl").REPLServer();
    Object.assign(repl.context, {
        client,
        db,
        uid,
        find_gaps,
        walk_gaps,
        seek,
        grab
    });
    repl.on("exit", terminate);
})();
