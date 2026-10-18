// Thermostat War 3D: the online game server ("relay") for the browser version.
//
// Browsers can't connect to each other directly, so in the browser version every multiplayer message
// travels player -> this server -> other player. The host asks for a room and gets a short code
// (like "KQ7P"), friends join with that code, and this server passes messages between them.
// It knows nothing about the game rules: the host's game still decides everything.
//
// Run it with:  node relay.js   (listens on the PORT environment variable, default 8070).
// No extra packages needed: the small WebSocket part is written out below.
'use strict';
const http = require('http');
const crypto = require('crypto');

const PORT = Number(process.env.PORT) || 8070;
const MAX_PLAYERS = 4; // Host + 3 guests per room.
const MAX_ROOMS = 500;
const MAX_MESSAGE = 256 * 1024; // Bytes. Game messages are far smaller.
const PING_EVERY_MS = 20000; // Keeps quiet connections (waiting in the lobby) open.
const SILENT_LIMIT_MS = 70000; // A player we haven't heard from for this long is gone.
const CODE_LETTERS = 'ABCDEFGHJKMNPQRSTVWXYZ23456789'; // No 0/O, 1/I/L, U (easy to misread).
const WS_GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11'; // Fixed value from the WebSocket standard.

const rooms = new Map(); // Room code -> { code, host, guests: Map(id -> connection) }

// ------------------------------------------------------------------ web server

const server = http.createServer((request, response) => {
	// A plain web page so hosting services (and people) can check the server is awake.
	response.writeHead(200, { 'Content-Type': 'text/plain', 'Access-Control-Allow-Origin': '*' });
	response.end(`Thermostat War 3D game server is running. Rooms open: ${rooms.size}\n`);
});

server.on('upgrade', (request, socket) => {
	const key = request.headers['sec-websocket-key'];
	if (!key || String(request.headers.upgrade).toLowerCase() !== 'websocket') {
		socket.end('HTTP/1.1 400 Bad Request\r\n\r\n');
		return;
	}
	const accept = crypto.createHash('sha1').update(key + WS_GUID).digest('base64');
	socket.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n'
		+ `Sec-WebSocket-Accept: ${accept}\r\n\r\n`);
	socket.setNoDelay(true);
	const conn = { socket, buffer: Buffer.alloc(0), parts: [], partsType: 0, room: null, id: 0,
		lastHeard: Date.now(), closed: false };
	socket.on('data', (data) => onSocketData(conn, data));
	socket.on('close', () => onClosed(conn));
	socket.on('error', () => onClosed(conn));
});

// ------------------------------------------------------------------ WebSocket frames

function onSocketData(conn, data) {
	conn.lastHeard = Date.now();
	conn.buffer = Buffer.concat([conn.buffer, data]);
	while (!conn.closed) {
		const buffer = conn.buffer;
		if (buffer.length < 2) return;
		const final = (buffer[0] & 0x80) !== 0;
		const type = buffer[0] & 0x0f;
		const masked = (buffer[1] & 0x80) !== 0;
		let length = buffer[1] & 0x7f;
		let offset = 2;
		if (length === 126) {
			if (buffer.length < 4) return;
			length = buffer.readUInt16BE(2);
			offset = 4;
		} else if (length === 127) {
			if (buffer.length < 10) return;
			const big = buffer.readBigUInt64BE(2);
			if (big > BigInt(MAX_MESSAGE)) return drop(conn);
			length = Number(big);
			offset = 10;
		}
		if (!masked || length > MAX_MESSAGE) return drop(conn); // Browsers and Godot always mask.
		if (buffer.length < offset + 4 + length) return;
		const mask = buffer.subarray(offset, offset + 4);
		const payload = Buffer.from(buffer.subarray(offset + 4, offset + 4 + length));
		for (let i = 0; i < payload.length; i++) payload[i] ^= mask[i & 3];
		conn.buffer = buffer.subarray(offset + 4 + length);
		onFrame(conn, final, type, payload);
	}
}

function onFrame(conn, final, type, payload) {
	if (type === 8) { // Close.
		sendFrame(conn, 8, Buffer.alloc(0));
		return drop(conn);
	}
	if (type === 9) return sendFrame(conn, 10, payload); // Ping -> pong.
	if (type === 10) return; // Pong (just proof they're alive).
	// A message may arrive in several pieces; put them together.
	if (type === 1 || type === 2) {
		conn.parts = [payload];
		conn.partsType = type;
	} else if (type === 0) {
		conn.parts.push(payload);
	} else {
		return drop(conn);
	}
	if (!final) return;
	const message = Buffer.concat(conn.parts);
	conn.parts = [];
	if (message.length > MAX_MESSAGE) return drop(conn);
	if (conn.partsType === 1) onText(conn, message.toString('utf8'));
	else onBinary(conn, message);
}

function sendFrame(conn, type, payload) {
	if (conn.closed) return;
	let header;
	if (payload.length < 126) {
		header = Buffer.from([0x80 | type, payload.length]);
	} else if (payload.length < 65536) {
		header = Buffer.alloc(4);
		header[0] = 0x80 | type;
		header[1] = 126;
		header.writeUInt16BE(payload.length, 2);
	} else {
		header = Buffer.alloc(10);
		header[0] = 0x80 | type;
		header[1] = 127;
		header.writeBigUInt64BE(BigInt(payload.length), 2);
	}
	conn.socket.write(Buffer.concat([header, payload]));
}

function sendText(conn, object) {
	sendFrame(conn, 1, Buffer.from(JSON.stringify(object), 'utf8'));
}

function drop(conn) {
	if (conn.closed) return;
	try { conn.socket.end(); } catch (error) { /* already gone */ }
	onClosed(conn);
}

// ------------------------------------------------------------------ rooms

// Control messages (text): {"op":"host"}, {"op":"join","room":"KQ7P","id":12345}, {"op":"kick","id":12345}.
function onText(conn, text) {
	let message;
	try { message = JSON.parse(text); } catch (error) { return drop(conn); }
	if (message.op === 'host' && !conn.room) {
		if (rooms.size >= MAX_ROOMS) return refuse(conn, 'busy');
		let code = String(message.room || '').toUpperCase(); // A fixed code is only used by automatic tests.
		if (!/^[A-Z0-9]{4,8}$/.test(code) || rooms.has(code)) code = newCode();
		const room = { code, host: conn, guests: new Map() };
		rooms.set(code, room);
		conn.room = room;
		conn.id = 1;
		sendText(conn, { op: 'hosted', room: code, id: 1 });
	} else if (message.op === 'join' && !conn.room) {
		const room = rooms.get(String(message.room || '').toUpperCase());
		const id = Number(message.id);
		if (!room) return refuse(conn, 'no_room');
		if (room.guests.size + 1 >= MAX_PLAYERS) return refuse(conn, 'full');
		if (!Number.isInteger(id) || id < 2 || id > 2147483647 || room.guests.has(id)) return refuse(conn, 'bad_id');
		room.guests.set(id, conn);
		conn.room = room;
		conn.id = id;
		sendText(conn, { op: 'joined', room: room.code, id });
		sendText(room.host, { op: 'peer_join', id });
	} else if (message.op === 'kick' && conn.room && conn.id === 1) {
		const guest = conn.room.guests.get(Number(message.id));
		if (guest) refuse(guest, 'kicked');
	}
}

// Game messages (binary): 4 bytes "to whom" (0 = everyone, -N = everyone except N), then the data.
// The server swaps the first 4 bytes for "from whom" and passes it on.
function onBinary(conn, message) {
	const room = conn.room;
	if (!room || message.length < 4) return;
	const target = message.readInt32LE(0);
	const out = Buffer.from(message);
	out.writeInt32LE(conn.id, 0);
	if (conn.id !== 1) { // Guests only ever talk to the host.
		if (target === 1) sendFrame(room.host, 2, out);
		return;
	}
	if (target > 0) {
		const guest = room.guests.get(target);
		if (guest) sendFrame(guest, 2, out);
		return;
	}
	for (const [id, guest] of room.guests) {
		if (target === 0 || id !== -target) sendFrame(guest, 2, out);
	}
}

// Says why (as a message, and again in the closing handshake), then hangs up.
function refuse(conn, reason) {
	sendText(conn, { op: 'error', reason });
	const close = Buffer.alloc(2 + Buffer.byteLength(reason));
	close.writeUInt16BE(4000, 0);
	close.write(reason, 2);
	sendFrame(conn, 8, close);
	drop(conn);
}

function onClosed(conn) {
	if (conn.closed) return;
	conn.closed = true;
	const room = conn.room;
	conn.room = null;
	if (!room) return;
	if (conn.id === 1) { // The host left: the game is over for everyone in the room.
		rooms.delete(room.code);
		for (const guest of room.guests.values()) {
			guest.room = null;
			refuse(guest, 'host_left');
		}
	} else if (room.guests.get(conn.id) === conn) {
		room.guests.delete(conn.id);
		sendText(room.host, { op: 'peer_leave', id: conn.id });
	}
}

function newCode() {
	for (;;) {
		let code = '';
		for (let i = 0; i < 4; i++) code += CODE_LETTERS[crypto.randomInt(CODE_LETTERS.length)];
		if (!rooms.has(code)) return code;
	}
}

// Every so often: ping everyone (keeps connections open) and drop players who have gone silent.
setInterval(() => {
	const now = Date.now();
	for (const room of rooms.values()) {
		for (const conn of [room.host, ...room.guests.values()]) {
			if (now - conn.lastHeard > SILENT_LIMIT_MS) drop(conn);
			else sendFrame(conn, 9, Buffer.alloc(0));
		}
	}
}, PING_EVERY_MS);

server.listen(PORT, () => console.log(`Thermostat War 3D game server listening on port ${PORT}`));
