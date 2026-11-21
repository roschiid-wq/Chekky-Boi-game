// Descend Further VR - tiny multiplayer server. No dependencies: `node server.js`
// Serves the game (index.html) and relays player state over WebSocket at /ws
const http=require('http'),fs=require('fs'),path=require('path'),crypto=require('crypto');
const PORT=process.env.PORT||3000,MAXP=16,TOPICS=new Set(['shot','kill','revive','plat']);
const GUID='258EAFA5-E914-47DA-95CA-C5AB0DC85B11',rooms=new Map();
const html=()=>fs.readFileSync(path.join(__dirname,'index.html'));
const srv=http.createServer((req,res)=>{
  if(req.url==='/health'){res.end('ok');return}
  if(req.url.split('?')[0]==='/'||req.url.startsWith('/index')){res.writeHead(200,{'content-type':'text/html; charset=utf-8','cache-control':'no-cache'});res.end(html());return}
  res.writeHead(404);res.end('not found')});
function frame(str){const b=Buffer.from(str),n=b.length;let h;
  if(n<126)h=Buffer.from([0x81,n]);else if(n<65536){h=Buffer.alloc(4);h[0]=0x81;h[1]=126;h.writeUInt16BE(n,2)}else{h=Buffer.alloc(10);h[0]=0x81;h[1]=127;h.writeBigUInt64BE(BigInt(n),2)}
  return Buffer.concat([h,b])}
function send(c,o){if(c.sock.destroyed)return;try{c.sock.write(frame(JSON.stringify(o)))}catch(e){}}
const isN=a=>Array.isArray(a)&&a.length<=12&&a.every(x=>typeof x==='number'&&isFinite(x));
function cleanP(d){if(!d||typeof d!=='object'||Array.isArray(d))return null;const o={};
  for(const k of ['h','l','r','m']){if(d[k]!==undefined){if(!isN(d[k]))return null;o[k]=d[k]}}
  for(const k of ['v','a','p']){if(d[k]!==undefined)o[k]=d[k]?1:0}return o}
function cleanE(k,d){if(!TOPICS.has(k)||!d||typeof d!=='object')return null;
  if(k==='shot')return isN(d.o)&&isN(d.e)?{o:d.o,e:d.e}:null;
  if(k==='plat'){if(typeof d.id!=='string'||d.id.length>16)return null;const o={id:d.id,a:d.a?1:0};if(o.a){for(const q of ['x','y','z','s']){if(typeof d[q]!=='number'||!isFinite(d[q]))return null;o[q]=d[q]}}return o}
  return{}}
srv.on('upgrade',(req,sock)=>{
  const u=new URL(req.url,'http://x');if(u.pathname!=='/ws'||!req.headers['sec-websocket-key']){sock.destroy();return}
  const rn=(u.searchParams.get('r')||'main').replace(/[^\w-]/g,'').slice(0,24)||'main';
  let room=rooms.get(rn);if(!room){room={n:rn,cl:new Map(),dirty:false};rooms.set(rn,room)}
  if(room.cl.size>=MAXP){sock.write('HTTP/1.1 503 Full\r\n\r\n');sock.destroy();return}
  sock.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: '+crypto.createHash('sha1').update(req.headers['sec-websocket-key']+GUID).digest('base64')+'\r\n\r\n');
  sock.setNoDelay(true);
  const id=crypto.randomBytes(4).toString('hex'),c={id,sock,pres:null,buf:Buffer.alloc(0),alive:true,ev:0,evT:Date.now(),room};
  room.cl.set(id,c);send(c,{t:'hello',id});room.dirty=true;
  const drop=()=>{if(room.cl.get(id)===c){room.cl.delete(id);room.dirty=true;if(!room.cl.size)rooms.delete(rn)}sock.destroy()};
  sock.on('data',d=>{c.buf=Buffer.concat([c.buf,d]);if(c.buf.length>20000){drop();return}
    for(;;){const b=c.buf;if(b.length<2)break;const op=b[0]&15,fin=b[0]&128,mk=b[1]&128;let n=b[1]&127,o=2;
      if(n===126){if(b.length<4)break;n=b.readUInt16BE(2);o=4}else if(n===127){if(b.length<10)break;n=Number(b.readBigUInt64BE(2));o=10}
      if(!mk||n>8000||!fin){drop();return}
      if(b.length<o+4+n)break;const key=b.slice(o,o+4),p=Buffer.from(b.slice(o+4,o+4+n));for(let i=0;i<n;i++)p[i]^=key[i&3];
      c.buf=b.slice(o+4+n);
      if(op===8){drop();return}
      if(op===9){try{sock.write(Buffer.concat([Buffer.from([0x8a,n]),p]))}catch(e){}continue}
      if(op===10){c.alive=true;continue}
      if(op!==1)continue;let m;try{m=JSON.parse(p.toString())}catch(e){continue}
      if(!m||typeof m!=='object')continue;
      if(m.t==='p'){const q=cleanP(m.d);if(q){c.pres=q;room.dirty=true}}
      else if(m.t==='e'){const now=Date.now();if(now-c.evT>1000){c.evT=now;c.ev=0}if(++c.ev>60)continue;const q=cleanE(m.k,m.d);if(q)for(const o2 of room.cl.values())if(o2!==c)send(o2,{t:'e',k:m.k,d:q,peer:id})}
    }});
  sock.on('close',drop);sock.on('error',drop);
});
setInterval(()=>{for(const room of rooms.values()){if(!room.dirty)continue;room.dirty=false;
  const all=[...room.cl.values()].map(c=>({peer:c.id,presence:c.pres}));
  for(const c of room.cl.values())send(c,{t:'peers',list:all.filter(x=>x.peer!==c.id)})}},66);
setInterval(()=>{for(const room of rooms.values())for(const c of room.cl.values()){if(!c.alive){c.sock.destroy();continue}c.alive=false;try{c.sock.write(Buffer.from([0x89,0]))}catch(e){}}},20000);
srv.listen(PORT,()=>console.log('Descend Further server on :'+PORT));
