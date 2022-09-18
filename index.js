// npm i better-sqlite3 github:sovlet/sq-lib
const db = require('better-sqlite3')('./profiles.db')
const { GameClient, ClientData, Logger } = require("sq-lib")

const GAPS_MODE = false

let mask = 0 | 4 | 8 | 16 | 128 | 256 | 1024 | 65536, logNet = false, uid, session = "ВАШ ТОКЕН"
const guardReference = Buffer.from([6, 0, 0, 0, 2, 0, 0, 0, 0, 0])

db.prepare("CREATE TABLE IF NOT EXISTS profiles (uid INTEGER PRIMARY KEY, level INTEGER, shaman_level INTEGER, exp INTEGER, shaman_exp INTEGER, name TEXT, profile TEXT, sex INTEGER, moderator INTEGER, clan_id INTEGER, bdate INTEGER)").run()

function terminate() {
    console.log('Завершение работы...')
    db.close()
    process.exit()
}

process.on('SIGINT', terminate)
process.on('SIGTERM', terminate)

Logger.setOptions({ logFile: 0, debug: 0, info: 1, warn: 1, error: 1, fatal: 1 })
function log(...args) {
    let date = new Date().toLocaleTimeString('ru-RU', {
        hour12: false,
        hour: "numeric",
        minute: "numeric",
        second: "numeric"
    })
    Logger.info('net', date, ...args)
}

function createClient(host, ports) {
    let client = new GameClient({
        port: ports[Math.floor(Math.random() * ports.length)],
        host: host
    })
    return client
}

const client = createClient('88.212.206.137', ['11111', '11211', '11311'])
client.on('client.connect', () => handleConnect(client))
client.on('client.close', () => handleClose(client))
client.on('packet.incoming', (packet, buffer) => handlePacket(packet, buffer))
client.on('packet.incoming', (packet, buffer) => logPacket(packet, buffer, 0))
client.on('packet.outcoming', (packet, buffer) => logPacket(packet, buffer, 1))
client.setMaxListeners(0)
client.open()

function waitForResult(emitter, event, type, timeout = 0) {
    const onPacket = (resolve, packet) => {
        if (typeof (type) === 'function' && type(packet) || packet.type === type) {
            emitter.off(event, onPacket)
            clearTimeout(timeoutId)
            resolve(packet)
        }
    }
    let timeoutId
    return new Promise(
        (resolve, reject) => {
            emitter.on(event, onPacket.bind(this, resolve))
            if (timeout > 0) {
                timeoutId = setTimeout(() => {
                    emitter.off(event, onPacket)
                    console.error('Timeout', type)
                    reject(new Error('Timeout'))
                }, timeout)
            }
        }
    )
}

function executeAndWait(emitter, func, event, type, timeout = 0) {
    const promise = waitForResult(emitter, event, type, timeout)
    func()
    return promise
}

function queryStringToObject(queryString) {
    let result = {}
    queryString.split('&').forEach(item => {
        let [key, value] = item.split('=')
        result[key] = value
    })
    return result
}

function composeLogin(token) {
    const session = queryStringToObject(token)
    let id, netType, OAuth, key, tag, ref, result = []
    id = BigInt(session.userId)
    netType = parseInt(session.net_type)
    OAuth = session.OAuth ? 1 : 0
    switch (session.useApiType) {
        case 'sa':
            key = session.authKey
            ref = -1
            break
        case 'ok':
            key = session.auth_sig
            ref = 20000
            break
        case 'vk':
            key = ""
            ref = 0
            break
        case 'mm':
            key = ""
            ref = 10000
    }
    result = [id, netType, OAuth, key, 3, ref]
    if (session.useApiType !== 'sa')
        result.push(session.token)
    return result
}

function experienceToLevel(exp) {
    const levels = ClientData.ConfigData.player.levels
    for (let i = 0; i < levels.length; i++) {
        if (exp < levels[i].experience)
            return i - 1
    }
    return ClientData.ConfigData.player.MAX_LEVEL
}

function shamanExperienceToLevel(exp) {
    const levels = ClientData.ConfigData.shaman.levels
    for (let i = 0; i < levels.length; i++) {
        if (exp < levels[i])
            return i + 1
    }
    return ClientData.ConfigData.shaman.MAX_LEVEL + 1
}

function logPacket(packet, buffer, out) {
    if (out) {
        clearInterval(global.pingInterval)
        global.pingInterval = setInterval(() => {
            client.sendData('PING', 0)
        }, 30000)
    }
    if (logNet)
        log(packet, JSON.stringify(buffer))
}

async function handleClose() {
    log('Сервер закрыл соединение')
    process.exit(1)
}

async function handleConnect() {
    client.sendData('HELLO')
    let login = { data: { status: 2 } }
    while (login.data.status === 2) {
        login = await executeAndWait(
            client,
            () => client.sendData('LOGIN', ...composeLogin(session)),
            'packet.incoming',
            'PacketLogin',
            1000)
    }
    uid = login.data.innerId
    global.i = db.prepare("SELECT MAX(uid) AS uid FROM profiles WHERE uid != ?").get(uid).uid
    if (login.data.status !== 0) {
        log(uid, 'Закрываем сокет')
        client.close()
        return
    } else log(uid, 'статус логина:' + login.data.status)
    if (GAPS_MODE)
        find_gaps()
    else
        seek()
}

var notReceived = 0

async function grab(start, paging) {
    let ids = new Array(paging)
    for (var j = 0; j < paging; j++) {
        ids[j] = [start + j]
    }
    console.log('Запрос профилей с ' + start + ' по ' + (start + paging - 1))
    client.sendData('REQUEST', ids, mask)
}

let paging = 1000
async function seek() {
    await grab(i, paging)
    notReceived += 1
    global.i += paging
}

const insert = db.prepare('INSERT or REPLACE INTO profiles (uid, level, shaman_level, exp, shaman_exp, name, profile, sex, moderator, clan_id, bdate) VALUES (@1, @2, @3, @4, @5, @6, @7, @8, @9, @10, @11)')
const insertMany = db.transaction((profiles) => {
    for (const profile of profiles) insert.run(profile)
})

async function handlePacket(packet, buffer) {
    switch (packet.type) {
        case 'PacketGuard':
            if (Buffer.compare(buffer, guardReference) !== 0) {
                log(this.self.uid, 'Гард изменился, выходим.')
                process.exit()
            }
            client.sendData('GUARD', [])
            break
        case 'PacketInfo':
            notReceived -= 1
            const profiles = packet.data.data
            const pendingUpdate = []
            let lastProfile = 0
            for (var i = 0; i < profiles.length; i++) {
                const profile = profiles[i]
                if (lastProfile < profile.uid && lastProfile != uid) lastProfile = profile.uid
                log('Получили информацию об игроке', profile.uid)
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
                })
            }
            insertMany(pendingUpdate)
            if (notReceived != 0 && global.i - lastProfile < 20 && !GAPS_MODE) await seek()
            break
    }
}

function find_gaps() {
    log('Ищем пропуски...')
    const uids = db.prepare("SELECT uid FROM profiles").all().map((x) => x.uid)
    const maxId = uids[uids.length - 1]
    let index = 0, x = 1, gaps = [];
    while (index < uids.length && x <= maxId) {
        if (uids[index] != x)
            gaps.push([x])
        else
            index++;
        x++;
    }
    client.sendData('REQUEST', gaps, mask)
    log('Найдено пропусков: ' + gaps.length + '. Запрос отправлен.')
}

(async () => {
    const repl = require('repl').REPLServer()
    Object.assign(repl.context, { client, seek, db, uid })
    repl.on('exit', terminate)
})()