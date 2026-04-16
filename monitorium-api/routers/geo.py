from fastapi import APIRouter
import httpx
import cache

router = APIRouter(prefix="/geo", tags=["geo"])

OPENSKY_URL = (
    "https://opensky-network.org/api/states/all"
    "?lamin=40&lomin=48&lamax=56&lomax=88"
)
CASPIAN_VESSELS_URL = (
    "https://www.myshiptracking.com/requests/vesselsonmap.php"
    "?type=json&minlat=36&maxlat=47&minlon=49&maxlon=55"
)


@router.get("/flights", summary="Live flight positions over Kazakhstan")
async def get_flights():
    cached = cache.get("geo:flights", 30)
    if cached is not None:
        return cached

    try:
        async with httpx.AsyncClient(
            timeout=10,
            headers={"User-Agent": "Mozilla/5.0 (compatible; Monitorium/1.0)"},
        ) as client:
            resp = await client.get(OPENSKY_URL)
            resp.raise_for_status()
            data = resp.json()

        states = data.get("states") or []
        # OpenSky state vector indices:
        # 0=icao24, 1=callsign, 5=lon, 6=lat, 7=baro_alt, 9=velocity, 10=true_track
        result = [
            {
                "icao":     s[0],
                "callsign": (s[1] or "").strip(),
                "lat":      s[6],
                "lon":      s[5],
                "alt":      s[7],
                "speed":    s[9],
                "heading":  s[10],
            }
            for s in states
            if s[5] is not None and s[6] is not None
        ]
    except Exception:
        result = []  # graceful degradation — frontend shows 0 flights

    cache.set("geo:flights", result)
    return result


@router.get("/vessels", summary="Vessel positions in the Caspian Sea")
async def get_vessels():
    cached = cache.get("geo:vessels", 60)
    if cached is not None:
        return cached

    try:
        async with httpx.AsyncClient(
            timeout=10,
            headers={"User-Agent": "Mozilla/5.0 (compatible; Monitorium/1.0)"},
        ) as client:
            resp = await client.get(CASPIAN_VESSELS_URL)
            resp.raise_for_status()
        raw = resp.json()
        # Normalize to consistent schema regardless of source format
        result = []
        for v in (raw if isinstance(raw, list) else raw.get("vessels", [])):
            result.append({
                "mmsi":    str(v.get("mmsi", "")),
                "name":    v.get("name") or v.get("shipname", ""),
                "lat":     v.get("lat") or v.get("latitude"),
                "lon":     v.get("lon") or v.get("longitude"),
                "type":    v.get("type") or v.get("ship_type", ""),
                "speed":   v.get("speed") or v.get("sog"),
                "heading": v.get("heading") or v.get("cog"),
            })
        result = [v for v in result if v["lat"] and v["lon"]]
    except Exception:
        result = []  # graceful degradation — frontend shows 0 vessels

    cache.set("geo:vessels", result)
    return result
