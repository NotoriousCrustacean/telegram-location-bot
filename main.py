import asyncio, hashlib, html, io, json, math, os, re, time
from datetime import datetime, timezone, timedelta
from pathlib import Path

import httpx
from timezonefinder import TimezoneFinder
from zoneinfo import ZoneInfo
from openpyxl import Workbook
from openpyxl.styles import Font

from telegram import Update, KeyboardButton, ReplyKeyboardMarkup, ReplyKeyboardRemove, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.error import Forbidden, BadRequest
from telegram.ext import ApplicationBuilder, ContextTypes, CommandHandler, MessageHandler, CallbackQueryHandler, filters

TOKEN=os.environ.get("TELEGRAM_TOKEN","").strip()
CLAIM_CODE=os.environ.get("CLAIM_CODE","").strip()
STATE_FILE=Path(os.environ.get("STATE_FILE","state.json"))
TRIGGERS={t.strip().lower() for t in os.environ.get("TRIGGERS","eta,1717").split(",") if t.strip()}
UA=os.environ.get("NOMINATIM_USER_AGENT","dispatch-eta-bot/1.0").strip()
NOMINATIM_MIN=float(os.environ.get("NOMINATIM_MIN_INTERVAL","1.1"))
ETA_ALL_MAX=int(os.environ.get("ETA_ALL_MAX","6"))
DELETEALL_DEFAULT=int(os.environ.get("DELETEALL_DEFAULT","300"))

TF=TimezoneFinder()
NOM_URL="https://nominatim.openstreetmap.org/search"
OSRM_URL="https://router.project-osrm.org/route/v1/driving/{lon1},{lat1};{lon2},{lat2}"

_lock=asyncio.Lock()
_geo_lock=asyncio.Lock()
_geo_last=0.0

def now(): return datetime.now(timezone.utc)
def now_iso(): return now().isoformat()
def h(x): return html.escape("" if x is None else str(x), quote=False)
def tzinfo(name):
    try: return ZoneInfo(name)
    except Exception: return timezone.utc

def load_state():
    if STATE_FILE.exists():
        try: st=json.loads(STATE_FILE.read_text("utf-8"))
        except Exception: st={}
    else: st={}
    st.setdefault("owner",None)
    st.setdefault("allowed",[])
    st.setdefault("last",None)     # {lat,lon,tz,at}
    st.setdefault("job",None)
    st.setdefault("focus_i",0)
    st.setdefault("gc",{})         # addr -> {lat,lon,tz}
    st.setdefault("hist",[])       # catalog records
    return st

def save_state(st):
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp=STATE_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(st,ensure_ascii=False),encoding="utf-8")
    tmp.replace(STATE_FILE)

def is_owner(update, st):
    u=update.effective_user
    return bool(u and st.get("owner") and u.id==st["owner"])

def allowed(update, st):
    chat=update.effective_chat
    if not chat: return False
    if chat.type=="private": return is_owner(update, st)
    return chat.id in set(st.get("allowed") or [])

def fmt_dur(s):
    s=max(0,int(s)); m=s//60; h_=m//60; m%=60
    return f"{h_}h {m}m" if h_ else f"{m}m"

def fmt_mi(m):
    mi=m/1609.344
    return f"{mi:.1f} mi" if mi<10 else f"{mi:.0f} mi"

def hav_m(lat1,lon1,lat2,lon2):
    R=6371000.0
    p1,p2=math.radians(lat1),math.radians(lat2)
    dp=math.radians(lat2-lat1); dl=math.radians(lon2-lon1)
    a=math.sin(dp/2)**2+math.cos(p1)*math.cos(p2)*math.sin(dl/2)**2
    return 2*R*math.asin(math.sqrt(a))

def fallback_s(dist_m):
    km=dist_m/1000
    sp=55 if km<80 else 85 if km<320 else 105
    return (km/sp)*3600

def addr_variants(addr):
    a=" ".join((addr or "").split())
    if not a: return []
    out=[a]
    parts=[p.strip() for p in a.split(",") if p.strip()]
    if len(parts)>=2: out.append(", ".join(parts[1:]))
    out.append(re.sub(r"\b(?:suite|ste|unit|#)\s*[\w\-]+\b","",a,flags=re.I).strip())
    if len(parts)>=2: out.append(", ".join(parts[-2:]))
    seen=set(); res=[]
    for x in out:
        x=" ".join(x.split())
        if x and x not in seen:
            seen.add(x); res.append(x)
    return res

async def geocode(st, addr):
    cache=st.get("gc") or {}
    if addr in cache:
        try:
            v=cache[addr]; return float(v["lat"]), float(v["lon"]), v.get("tz","UTC")
        except Exception:
            pass
    if not UA: return None
    headers={"User-Agent":UA}
    async with httpx.AsyncClient(timeout=15, headers=headers) as c:
        for q in addr_variants(addr):
            async with _geo_lock:
                global _geo_last
                wait=(_geo_last+NOMINATIM_MIN)-time.monotonic()
                if wait>0: await asyncio.sleep(wait)
                r=await c.get(NOM_URL, params={"q":q,"format":"jsonv2","limit":1})
                _geo_last=time.monotonic()
            if r.status_code>=400: continue
            js=r.json() or []
            if not js: continue
            lat,lon=float(js[0]["lat"]),float(js[0]["lon"])
            tz=TF.timezone_at(lat=lat, lng=lon) or "UTC"
            cache[addr]={"lat":lat,"lon":lon,"tz":tz}
            st["gc"]=cache
            async with _lock:
                st2=load_state(); st2.setdefault("gc",{}); st2["gc"][addr]=cache[addr]; save_state(st2)
            return lat,lon,tz
    return None

async def route(a,b):
    url=OSRM_URL.format(lon1=a[1],lat1=a[0],lon2=b[1],lat2=b[0])
    try:
        async with httpx.AsyncClient(timeout=15) as c:
            r=await c.get(url, params={"overview":"false"})
            if r.status_code>=400: return None
            js=r.json() or {}
            routes=js.get("routes") or []
            if not routes: return None
            return float(routes[0]["distance"]), float(routes[0]["duration"])
    except Exception:
        return None

async def eta_to(st, origin, label, addr):
    g=await geocode(st, addr)
    if not g: return {"ok":False,"err":f"Couldn't locate {label}."}
    dest=(g[0],g[1])
    r=await route(origin, dest)
    if r: return {"ok":True,"m":r[0],"s":r[1],"method":"osrm","tz":g[2]}
    dist=hav_m(origin[0],origin[1],dest[0],dest[1])
    return {"ok":True,"m":dist,"s":fallback_s(dist),"method":"approx","tz":g[2]}

RATE_RE=re.compile(r"\b(?:RATE|PAY)\b\s*:\s*\$?\s*([0-9][0-9,]*(?:\.[0-9]{1,2})?)", re.I)
MILES_RE=re.compile(r"\b(?:LOADED|MILES)\b\s*:\s*([0-9][0-9,]*)", re.I)
PU_TIME_RE=re.compile(r"^\s*PU time:\s*(.+)$", re.I)
DEL_TIME_RE=re.compile(r"^\s*DEL time:\s*(.+)$", re.I)
PU_ADDR_RE=re.compile(r"^\s*PU Address\s*:\s*(.*)$", re.I)
DEL_ADDR_RE=re.compile(r"^\s*DEL Address(?:\s*\d+)?\s*:\s*(.*)$", re.I)
LOAD_NUM_RE=re.compile(r"^\s*Load Number\s*:\s*(.+)$", re.I)
PICKUP_RE=re.compile(r"^\s*Pickup\s*:\s*(.+)$", re.I)
DELIVERY_RE=re.compile(r"^\s*Delivery\s*:\s*(.+)$", re.I)
TIMEISH=re.compile(r"\b(\d{1,2}[/-]\d{1,2}[/-]\d{2,4}|\d{4}-\d{2}-\d{2}|\d{1,2}:\d{2})\b")

def extract_rate_miles(text):
    rate=miles=None
    m=RATE_RE.search(text)
    if m:
        try: rate=float(m.group(1).replace(",",""))
        except Exception: pass
    m=MILES_RE.search(text)
    if m:
        try: miles=int(m.group(1).replace(",",""))
        except Exception: pass
    return rate,miles

def take_block(lines,i,first):
    out=[]
    if first.strip(): out.append(first.strip())
    j=i+1
    while j<len(lines):
        s=lines[j].strip()
        if not s: break
        low=s.lower()
        if low.startswith(("pu time:","del time:","pu address","del address","pickup:","delivery:")): break
        if set(s) <= {"-"} or set(s) <= {"="}: break
        out.append(s); j+=1
    return out,j

def init_job(job):
    job.setdefault("meta",{})
    pu=job.setdefault("pu",{})
    pu.setdefault("status",{"arr":None,"load":None,"dep":None,"comp":None})
    pu.setdefault("docs",{"pti":False,"bol":False})
    for d in job.setdefault("del",[]):
        d.setdefault("status",{"arr":None,"del":None,"dep":None,"comp":None,"skip":False})
        d.setdefault("docs",{"pod":False})
    return job

def parse_detailed(text):
    low=text.lower()
    if "pu address" not in low or "del address" not in low: return None
    lines=[ln.rstrip() for ln in text.splitlines()]
    pu_time=None; cur_del_time=None; pu_addr=None; pu_lines=None; dels=[]; load_num=None
    for i,ln in enumerate(lines):
        m=LOAD_NUM_RE.match(ln)
        if m: load_num=m.group(1).strip()
        m=PU_TIME_RE.match(ln)
        if m: pu_time=m.group(1).strip()
        m=DEL_TIME_RE.match(ln)
        if m: cur_del_time=m.group(1).strip()
        m=PU_ADDR_RE.match(ln)
        if m and not pu_addr:
            blk,_=take_block(lines,i,m.group(1))
            if blk: pu_lines=blk; pu_addr=", ".join(blk)
        m=DEL_ADDR_RE.match(ln)
        if m:
            blk,_=take_block(lines,i,m.group(1))
            if blk: dels.append({"addr":", ".join(blk),"lines":blk,"time":cur_del_time})
    if not pu_addr or not dels: return None
    rate,miles=extract_rate_miles(text)
    meta={"rate":rate,"miles":miles}
    if load_num: meta["load_number"]=load_num
    jid=hashlib.sha1((pu_addr+"|"+"|".join(d["addr"] for d in dels)).encode()).hexdigest()[:10]
    job={"id":jid,"meta":meta,"pu":{"addr":pu_addr,"lines":pu_lines or [pu_addr],"time":pu_time},"del":dels}
    return init_job(job)

def parse_summary(text):
    low=text.lower()
    if "pickup:" not in low or "delivery:" not in low: return None
    meta={}; pu_addr=pu_time=None; dels=[]; pending=None
    for ln in [x.strip() for x in text.splitlines() if x.strip()]:
        m=LOAD_NUM_RE.match(ln)
        if m: meta["load_number"]=m.group(1).strip(); continue
        m=PICKUP_RE.match(ln)
        if m:
            v=m.group(1).strip()
            if TIMEISH.search(v): pu_time=v
            else: pu_addr=v
            continue
        m=DELIVERY_RE.match(ln)
        if m:
            v=m.group(1).strip()
            if TIMEISH.search(v):
                if pending and not pending.get("time"): pending["time"]=v; pending=None
            else:
                pending={"addr":v,"lines":[v],"time":None}; dels.append(pending)
            continue
    if not pu_addr or not dels: return None
    rate,miles=extract_rate_miles(text)
    if rate is not None: meta["rate"]=rate
    if miles is not None: meta["miles"]=miles
    jid=hashlib.sha1((str(meta.get("load_number",""))+"|"+pu_addr+"|"+"|".join(d["addr"] for d in dels)).encode()).hexdigest()[:10]
    job={"id":jid,"meta":meta,"pu":{"addr":pu_addr,"lines":[pu_addr],"time":pu_time},"del":dels}
    return init_job(job)

def parse_job(text): return parse_detailed(text) or parse_summary(text)
def pu_done(job): return bool(job.get("pu",{}).get("status",{}).get("comp"))

def next_incomplete(job,start=0):
    for i,d in enumerate(job.get("del") or []):
        if i<start: continue
        if not (d.get("status") or {}).get("comp"): return i
    return None

def focus(job, st):
    if not pu_done(job): return ("PU",0)
    dels=job.get("del") or []
    if not dels: return ("DEL",0)
    i=max(0,min(int(st.get("focus_i",0) or 0), len(dels)-1))
    if (dels[i].get("status") or {}).get("comp"):
        ni=next_incomplete(job,i+1)
        if ni is not None: i=ni
    return ("DEL", i)

def short_place(lines, addr):
    for x in reversed(lines or []):
        x=x.strip()
        if x and len(x)<=60: return x
    return (addr or "").strip()

def b(text,data): return InlineKeyboardButton(text, callback_data=data)
def chk(on,label): return ("✅ "+label) if on else label

def kb(job, st):
    stage,i=focus(job, st)
    pu=job["pu"]; ps=pu["status"]; pd=pu["docs"]
    rows=[]
    if stage=="PU":
        rows.append([b(chk(bool(ps["arr"]),"Arrived PU"),"PU:A"),
                     b(chk(bool(ps["load"]),"Loaded"),"PU:L"),
                     b(chk(bool(ps["dep"]),"Departed"),"PU:D")])
        rows.append([b(chk(pd.get("pti"),"PTI"),"DOC:PTI"),
                     b(chk(pd.get("bol"),"BOL"),"DOC:BOL"),
                     b(chk(bool(ps["comp"]),"PU Complete"),"PU:C")])
    else:
        d=job["del"][i]; ds=d["status"]; dd=d["docs"]; lbl=f"DEL {i+1}/{len(job['del'])}"
        rows.append([b(chk(bool(ds["arr"]),"Arrived "+lbl),"DEL:A"),
                     b(chk(bool(ds["del"]),"Delivered"),"DEL:DL"),
                     b(chk(bool(ds["dep"]),"Departed"),"DEL:D")])
        rows.append([b(chk(dd.get("pod"),"POD"),"DOC:POD"),
                     b(chk(bool(ds["comp"]),"Stop Complete"),"DEL:C"),
                     b("Skip Stop","DEL:S")])
    rows.append([b("ETA","ETA:A"), b("ETA all","ETA:ALL")])
    rows.append([b("📊 Catalog","SHOW:CAT"), b("Finish Load","JOB:FIN")])
    return InlineKeyboardMarkup(rows)

def job_title(job):
    m=job.get("meta") or {}
    ln=m.get("load_number") or ""
    return f"Load {ln}" if ln else "Load"

async def start_cmd(update, ctx):
    await update.effective_message.reply_text(
        "Dispatch Bot\n\n"
        f"Triggers: {', '.join(sorted(TRIGGERS))}\n"
        "Setup: DM /claim <code> → DM /update → in group /allowhere\n"
        "Use: type eta / 1717 or /panel\n"
        "Catalog: /catalog • /finish",
        disable_web_page_preview=True
    )

async def claim(update, ctx):
    if update.effective_chat.type!="private":
        await update.effective_message.reply_text("DM me /claim <code>."); return
    if not CLAIM_CODE:
        await update.effective_message.reply_text("CLAIM_CODE missing."); return
    if " ".join(ctx.args or []).strip()!=CLAIM_CODE:
        await update.effective_message.reply_text("❌ Wrong code."); return
    async with _lock:
        st=load_state(); st["owner"]=update.effective_user.id; save_state(st)
    await update.effective_message.reply_text("✅ Owner set.")

async def allowhere(update, ctx):
    async with _lock:
        st=load_state()
        if not is_owner(update, st):
            await update.effective_message.reply_text("Owner only."); return
        chat=update.effective_chat
        if chat.type=="private":
            await update.effective_message.reply_text("Run /allowhere inside the group."); return
        s=set(st.get("allowed") or []); s.add(chat.id); st["allowed"]=sorted(s); save_state(st)
    await update.effective_message.reply_text("✅ Group allowed.")

async def update_cmd(update, ctx):
    async with _lock:
        st=load_state()
        if not is_owner(update, st):
            await update.effective_message.reply_text("Owner only."); return
    if update.effective_chat.type!="private":
        await update.effective_message.reply_text("DM /update (best)."); return
    kb_=[[KeyboardButton("📍 Send my current location", request_location=True)]]
    await update.effective_message.reply_text(
        "Tap to send your location.\nTip: Share Live Location too.",
        reply_markup=ReplyKeyboardMarkup(kb_, resize_keyboard=True, one_time_keyboard=True),
    )

async def on_loc(update, ctx):
    msg=update.effective_message
    if not msg or not msg.location: return
    async with _lock:
        st=load_state()
        if not is_owner(update, st): return
        lat,lon=msg.location.latitude,msg.location.longitude
        tz=TF.timezone_at(lat=lat, lng=lon) or "UTC"
        st["last"]={"lat":lat,"lon":lon,"tz":tz,"at":now_iso()}
        save_state(st)
    if update.message is not None:
        await msg.reply_text("✅ Location saved.", reply_markup=ReplyKeyboardRemove())

async def panel(update, ctx):
    async with _lock:
        st=load_state()
        if not allowed(update, st): return
        job=st.get("job")
        if not job:
            await update.effective_message.reply_text("No active load yet."); return
        job=init_job(job); st["job"]=job; save_state(st)
        stage,i=focus(job, st)
    await update.effective_message.reply_text(
        f"<b>{h(job_title(job))}</b>\nFocus: {h(stage)}{'' if stage=='PU' else ' '+str(i+1)}",
        parse_mode="HTML",
        reply_markup=kb(job, st),
    )

async def send_eta(update, ctx, which):
    async with _lock:
        st=load_state()
    if not allowed(update, st): return
    loc=st.get("last")
    if not loc:
        await update.effective_message.reply_text("No saved location yet. Owner: DM /update."); return
    origin=(float(loc["lat"]), float(loc["lon"]))
    tz_now=loc.get("tz","UTC")
    tz=tzinfo(tz_now)

    await ctx.bot.send_location(chat_id=update.effective_chat.id, latitude=origin[0], longitude=origin[1])

    job=st.get("job")
    if not job:
        await update.effective_message.reply_text(
            f"<b>⏱ ETA</b>\nNow: {h(datetime.now(tz).strftime('%Y-%m-%d %H:%M'))} ({h(tz_now)})\n\n<i>No active load yet.</i>",
            parse_mode="HTML",
        )
        return

    job=init_job(job)
    stage,i=focus(job, st)

    def stop_auto():
        if not pu_done(job): return ("Pickup", job["pu"]["addr"], job["pu"]["lines"])
        d=job["del"][i]; return (f"Delivery {i+1}/{len(job['del'])}", d["addr"], d.get("lines") or [])

    which=which.upper()
    if which=="ALL":
        out=[f"<b>⏱ ETA — {h(job_title(job))}</b>"]
        stops=[("PU", job["pu"]["addr"], job["pu"]["lines"])]
        for j,d in enumerate((job.get("del") or [])[:ETA_ALL_MAX]):
            stops.append((f"D{j+1}", d["addr"], d.get("lines") or []))
        for lab,addr,lines in stops:
            r=await eta_to(st, origin, lab, addr)
            place=short_place(lines, addr)
            if r.get("ok"):
                arr=(now().astimezone(tz)+timedelta(seconds=float(r["s"]))).strftime("%H:%M")
                tag=" (approx)" if r.get("method")=="approx" else ""
                out.append(f"<b>{h(lab)}:</b> <b>{h(fmt_dur(r['s']))}</b>{h(tag)} · {h(fmt_mi(r['m']))} · ~{h(arr)} — {h(place)}")
            else:
                out.append(f"<b>{h(lab)}:</b> ⚠️ {h(r.get('err'))} — {h(place)}")
        await update.effective_message.reply_text("\n".join(out), parse_mode="HTML", reply_markup=kb(job, st))
        return

    if which=="PU": label,addr,lines=("Pickup", job["pu"]["addr"], job["pu"]["lines"])
    elif which=="DEL" and pu_done(job) and (job.get("del") or []):
        d=job["del"][i]; label,addr,lines=(f"Delivery {i+1}/{len(job['del'])}", d["addr"], d.get("lines") or [])
    else:
        label,addr,lines=stop_auto()

    r=await eta_to(st, origin, label, addr)
    place=short_place(lines, addr)
    if r.get("ok"):
        arr=(now().astimezone(tz)+timedelta(seconds=float(r["s"]))).strftime("%H:%M")
        tag=" (approx)" if r.get("method")=="approx" else ""
        txt="\n".join([
            f"<b>⏱ ETA — {h(job_title(job))}</b>",
            f"<b>{h(label)}:</b> <b>{h(fmt_dur(r['s']))}</b>{h(tag)}",
            f"<b>Arrive ~</b> {h(arr)} ({h(tz_now)})",
            f"<b>Distance:</b> {h(fmt_mi(r['m']))}",
            f"<b>Target:</b> {h(place)}",
        ])
    else:
        txt=f"<b>⏱ ETA — {h(job_title(job))}</b>\n<b>{h(label)}:</b> ⚠️ {h(r.get('err'))}\n<b>Target:</b> {h(place)}"
    await update.effective_message.reply_text(txt, parse_mode="HTML", reply_markup=kb(job, st))

def week_key(dt):
    iso=dt.isocalendar(); return f"{iso.year}-W{iso.week:02d}"

async def est_miles(st, job):
    addrs=[job["pu"]["addr"]]+[d["addr"] for d in (job.get("del") or [])]
    coords=[]
    for a in addrs:
        g=await geocode(st,a)
        if not g: return None
        coords.append((g[0],g[1]))
    if len(coords)<2: return 0.0
    total=0.0
    for a,b in zip(coords, coords[1:]):
        r=await route(a,b)
        total += r[0] if r else hav_m(a[0],a[1],b[0],b[1])
    return total/1609.344

def make_xlsx(recs, title):
    wb=Workbook(); ws=wb.active; ws.title="Loads"
    ws.append([title]); ws["A1"].font=Font(bold=True, size=14)
    ws.append(["Week","Completed","Load #","Pickup","Deliveries","Rate","Posted Miles","Est Miles"])
    for c in ws[2]: c.font=Font(bold=True)
    tot_rate=0.0; tot_m=0.0
    for r in recs:
        ws.append([r.get("week"),r.get("completed"),r.get("load_number"),r.get("pickup"),r.get("deliveries"),r.get("rate"),r.get("posted_miles"),r.get("est_miles")])
        if r.get("rate") is not None: tot_rate += float(r["rate"])
        if r.get("est_miles") is not None: tot_m += float(r["est_miles"])
    ws.append([]); ws.append(["TOTAL","","","","",tot_rate,"",tot_m])
    for c in ws[ws.max_row]: c.font=Font(bold=True)
    bio=io.BytesIO(); wb.save(bio); return bio.getvalue()

async def finish(update, ctx):
    async with _lock:
        st=load_state()
        if not is_owner(update, st):
            await update.effective_message.reply_text("Owner only."); return
        job=st.get("j
