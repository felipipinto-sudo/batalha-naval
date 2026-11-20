const http = require("http");
const fs = require("fs");
const path = require("path");
const crypto = require("crypto");

const PORT = process.env.PORT || 3000;
const ROOT = path.join(__dirname, "public");
const rooms = new Map();
const clients = new Set();

const server = http.createServer((req, res) => {
  const pathname = decodeURIComponent(new URL(req.url, "http://localhost").pathname);
  const file = pathname === "/" ? "index.html" : pathname.replace(/^\/+/, "");
  const full = path.join(ROOT, file);
  if (!full.startsWith(ROOT)) { res.writeHead(403); return res.end("Forbidden"); }
  fs.readFile(full, (err, data) => {
    if (err) { res.writeHead(404); return res.end("Não encontrado"); }
    const ext = path.extname(full);
    const types = { ".html":"text/html; charset=utf-8", ".css":"text/css; charset=utf-8", ".js":"text/javascript; charset=utf-8" };
    res.writeHead(200, { "Content-Type": types[ext] || "application/octet-stream" });
    res.end(data);
  });
});

function frame(payload) {
  const data = Buffer.from(JSON.stringify(payload));
  let header;
  if (data.length < 126) header = Buffer.from([0x81, data.length]);
  else if (data.length < 65536) { header = Buffer.alloc(4); header[0]=0x81; header[1]=126; header.writeUInt16BE(data.length,2); }
  else { header = Buffer.alloc(10); header[0]=0x81; header[1]=127; header.writeBigUInt64BE(BigInt(data.length),2); }
  return Buffer.concat([header, data]);
}
function send(ws, obj) { if (ws.readyState === 1) ws.write(frame(obj)); }
function broadcast(room, obj) { for (const p of room.players) send(p.ws, obj); }
function removePlayer(ws) {
  const roomCode = ws.roomCode;
  if (!roomCode) return;
  const room = rooms.get(roomCode);
  if (!room) return;
  room.players = room.players.filter(p => p.ws !== ws);
  broadcast(room, { type: "opponent_left" });
  if (!room.players.length) rooms.delete(roomCode);
}
function handle(ws, msg) {
  let data; try { data = JSON.parse(msg); } catch { return; }
  if (data.type === "join") {
    const code = String(data.code || "").toUpperCase().replace(/[^A-Z0-9]/g,"").slice(0,8);
    const name = String(data.name || "Jogador").slice(0,20);
    if (!code) return send(ws, {type:"error", message:"Informe um código de sala."});
    let room = rooms.get(code);
    if (!room) { room = { players: [], started:false, ready:[] }; rooms.set(code, room); }
    if (room.players.length >= 2) return send(ws, {type:"error", message:"Essa sala já está cheia. Escolha outro código."});
    if (room.players.some(p => p.name === name)) return send(ws, {type:"error", message:"Use nomes diferentes para os jogadores."});
    const player = { ws, name, index: room.players.length };
    room.players.push(player); ws.roomCode = code; ws.playerIndex = player.index;
    send(ws, {type:"joined", player:player.index, names:room.players.map(p=>p.name)});
    if (room.players.length === 2) broadcast(room, {type:"room_ready", names:room.players.map(p=>p.name)});
    return;
  }
  const room = rooms.get(ws.roomCode);
  if (!room) return;
  if (data.type === "fleet") {
    room.fleets = room.fleets || {};
    room.fleets[ws.playerIndex] = data.cells;
    return;
  }
  if (data.type === "ready") {
    if (room.ready.includes(ws.playerIndex)) return;
    room.ready.push(ws.playerIndex);
    if (room.ready.length === 2) {
      room.started = true; room.turn = 0;
      broadcast(room, {type:"start", first:0, names:room.players.map(p=>p.name)});
    } else send(ws,{type:"waiting_ready"});
    return;
  }
  if (data.type === "action") {
    if (!room.started || ws.playerIndex !== room.turn) return send(ws,{type:"error",message:"Ainda não é seu turno."});
    room.shots = room.shots || {};
    room.shots[ws.playerIndex] = room.shots[ws.playerIndex] || [];
    if (room.shots[ws.playerIndex].some(p=>p.r===data.action.r && p.c===data.action.c)) return send(ws,{type:"error",message:"Você já atacou essa posição."});
    room.shots[ws.playerIndex].push(data.action);
    const opponent = room.players.find(p=>p.index !== ws.playerIndex);
    if (!opponent) return;
    const target = room.fleets && room.fleets[opponent.index] || [];
    const hit = target.some(([r,c]) => r === data.action.r && c === data.action.c);
    send(ws, {type:"attack_result", r:data.action.r, c:data.action.c, hit});
    send(opponent.ws, {type:"opponent_action", action:{r:data.action.r,c:data.action.c,hit}, by:ws.playerIndex});
    const hits = room.shots[ws.playerIndex].filter(p => target.some(([r,c])=>r===p.r&&c===p.c)).length;
    if (hits >= 19) { broadcast(room,{type:"game_over",winner:ws.playerIndex}); return; }
    room.turn = 1 - room.turn;
    broadcast(room, {type:"turn", player:room.turn});
    return;
  }
  if (data.type === "restart") {
    room.ready = []; room.started = false;
    broadcast(room,{type:"restart"});
  }
}
server.on("upgrade", (req, socket) => {
  if (req.headers.upgrade.toLowerCase() !== "websocket") return socket.destroy();
  const key = req.headers["sec-websocket-key"];
  const accept = crypto.createHash("sha1").update(key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").digest("base64");
  socket.write("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: " + accept + "\r\n\r\n");
  const ws = { write: b => socket.write(b), readyState:1, roomCode:null, playerIndex:null };
  clients.add(ws);
  let buffer = Buffer.alloc(0);
  socket.on("data", chunk => {
    buffer = Buffer.concat([buffer,chunk]);
    while (buffer.length >= 2) {
      const b1=buffer[0], b2=buffer[1]; let len=b2&127, offset=2;
      if (len===126) { if(buffer.length<4) return; len=buffer.readUInt16BE(2); offset=4; }
      else if(len===127) { if(buffer.length<10) return; len=Number(buffer.readBigUInt64BE(2)); offset=10; }
      const masked=!!(b2&128); if(masked) offset+=4;
      if(buffer.length<offset+len) return;
      let payload=buffer.subarray(offset,offset+len);
      if(masked) { const mask=buffer.subarray(offset-4,offset); payload=Buffer.from(payload); for(let i=0;i<payload.length;i++) payload[i]^=mask[i%4]; }
      buffer=buffer.subarray(offset+len);
      if((b1&15)===8) { socket.end(); return; }
      if((b1&15)===1) handle(ws,payload.toString());
    }
  });
  socket.on("close",()=>{ws.readyState=3; clients.delete(ws); removePlayer(ws);});
  socket.on("error",()=>{ws.readyState=3; clients.delete(ws); removePlayer(ws);});
});
server.listen(PORT, "0.0.0.0", () => console.log(`Batalha Naval disponível na porta ${PORT}`));
