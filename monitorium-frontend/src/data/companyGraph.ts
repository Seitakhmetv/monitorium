// KZ corporate ownership graph — real data as of 2024-2025
// Sources: company annual reports, KASE disclosures, public filings

export type EntityType =
  | 'government'   // Ministry / sovereign entity
  | 'fund'         // Samruk-Kazyna, Baiterek, etc.
  | 'soe'          // State-owned enterprise (may be listed)
  | 'listed'       // Publicly listed non-SOE
  | 'jv'           // Joint venture
  | 'foreign'      // Foreign company or fund
  | 'private'      // Privately held domestic

export interface Entity {
  id:          string
  name:        string
  shortName?:  string
  type:        EntityType
  ticker?:     string       // KASE/AIX ticker if listed
  sector?:     string
  country:     string       // ISO-2
  description: string
  website?:    string
  founded?:    number
}

export interface Edge {
  from:         string       // entity id (owner)
  to:           string       // entity id (subsidiary)
  stake:        number       // 0–100 percent
  label?:       string       // e.g. "direct", "via KazMunayGas"
  since?:       number       // year established
  note?:        string
}

// ─── Entities ────────────────────────────────────────────────────────────────

export const ENTITIES: Entity[] = [
  // Government
  {
    id: 'gov-kz',
    name: 'Government of the Republic of Kazakhstan',
    shortName: 'KZ Government',
    type: 'government',
    country: 'KZ',
    description: 'Sovereign owner of all state assets via Samruk-Kazyna National Welfare Fund and Baiterek Development Fund.',
  },

  // State funds
  {
    id: 'samruk',
    name: 'Samruk-Kazyna National Welfare Fund',
    shortName: 'Samruk-Kazyna',
    type: 'fund',
    country: 'KZ',
    description: 'Sovereign wealth fund created in 2008 to manage state stakes in strategic enterprises. Assets under management exceed $70 billion. Owns KMG, Kazatomprom, Air Astana parent, KTZ, Kazpost.',
    website: 'samruk-kazyna.kz',
    founded: 2008,
  },
  {
    id: 'baiterek',
    name: 'Baiterek National Management Holding',
    shortName: 'Baiterek',
    type: 'fund',
    country: 'KZ',
    description: 'Development finance arm of the Kazakh government. Controls DBK (Development Bank of Kazakhstan), KazAgro Finance, Kazyna Capital Management, and BCC-Invest.',
    website: 'baiterek.kz',
    founded: 2013,
  },
  {
    id: 'samruk-energy',
    name: 'Samruk-Energy',
    type: 'soe',
    country: 'KZ',
    sector: 'Utilities',
    description: "Samruk-Kazyna's electricity sub-holding. Controls Ekibastuz GRES-1 (jointly with Glencore), Ekibastuz GRES-2, KEGOC (national grid operator), and several HPP/wind assets.",
    founded: 2007,
  },

  // Oil & Gas (KMG tree)
  {
    id: 'kmg',
    name: 'KazMunayGas',
    shortName: 'KMG',
    type: 'soe',
    ticker: 'KMGZ',
    sector: 'Oil & Gas',
    country: 'KZ',
    description: "National oil & gas company of Kazakhstan. Operates upstream (Tengizchevroil JV, Kashagan NCOC JV, Karachaganak KPO JV), pipelines (KTO, KTG), and refining (Atyrau, Pavlodar refineries). World's 10th largest oil producer by country.",
    website: 'kmg.kz',
    founded: 2002,
  },
  {
    id: 'kto',
    name: 'KazTransOil',
    shortName: 'KTO',
    type: 'soe',
    ticker: 'KZTO',
    sector: 'Pipelines',
    country: 'KZ',
    description: 'National crude oil pipeline operator. Owns and operates 5,800 km of trunk oil pipelines including the Atyrau–Samara route and connections to the CPC pipeline system.',
    founded: 1997,
  },
  {
    id: 'ktg',
    name: 'KazTransGas',
    shortName: 'KTG',
    type: 'soe',
    sector: 'Pipelines',
    country: 'KZ',
    description: 'National gas pipeline operator. Manages ~20,000 km of gas transmission and distribution network. Supplies domestic gas and transit volumes from Turkmenistan to Russia/China.',
    founded: 2000,
  },
  {
    id: 'tco',
    name: 'Tengizchevroil',
    shortName: 'TCO',
    type: 'jv',
    sector: 'Oil & Gas',
    country: 'KZ',
    description: "JV operating the Tengiz oil field in Atyrau region — one of the world's deepest and highest-pressure super-giant fields. Produces ~700,000 bpd (post-FGP expansion 2023). Tengiz reserves estimated at 25-26 billion barrels.",
    founded: 1993,
  },
  {
    id: 'ncoc',
    name: 'North Caspian Operating Company',
    shortName: 'NCOC',
    type: 'jv',
    sector: 'Oil & Gas',
    country: 'KZ',
    description: "JV operating the Kashagan oil field in the northern Caspian Sea — the world's largest oil discovery since Prudhoe Bay (1968). Production ~400,000 bpd. Technically complex due to H₂S content. Cost overruns exceeded $50 billion.",
    founded: 2008,
  },
  {
    id: 'kpo',
    name: 'Karachaganak Petroleum Operating',
    shortName: 'KPO',
    type: 'jv',
    sector: 'Oil & Gas',
    country: 'KZ',
    description: 'JV operating the Karachaganak field in West Kazakhstan — a major oil/condensate and gas field. Produces ~220,000 boe/d. Gas transported to Orenburg (Russia) for processing. KMG acquired 10% stake in 2012.',
    founded: 1997,
  },

  // Uranium (Kazatomprom tree)
  {
    id: 'kzap',
    name: 'Kazatomprom',
    shortName: 'KZAP',
    type: 'soe',
    ticker: 'KZAP',
    sector: 'Uranium',
    country: 'KZ',
    description: "World's largest uranium producer (~23% of global supply). Operates 26 mines via JVs across South Kazakhstan. Dual-listed on LSE and AIX. Sells uranium oxide concentrate (U₃O₈) to nuclear utilities worldwide. Kazakhstan holds ~40% of identified global uranium reserves.",
    website: 'kazatomprom.kz',
    founded: 1997,
  },
  {
    id: 'jv-inkai',
    name: 'JV Inkai',
    shortName: 'Inkai',
    type: 'jv',
    sector: 'Uranium',
    country: 'KZ',
    description: 'Uranium mining JV in Turkestan region. Inkai deposit contains ~80,000 tU reserves. Production ~4,800 tU/year. In-situ leaching (ISL) method. KZAP owns 60%, Cameco 40%.',
    founded: 1997,
  },
  {
    id: 'jv-katco',
    name: 'JV Katco',
    shortName: 'Katco',
    type: 'jv',
    sector: 'Uranium',
    country: 'KZ',
    description: 'Uranium mining JV in South Kazakhstan (Moinkum + Tortkuduk deposits). Production ~4,000 tU/year. JV between Kazatomprom (49%) and Orano (formerly Areva, 51%).',
    founded: 1997,
  },
  {
    id: 'jv-budenovskoye',
    name: 'JV Budenovskoye',
    shortName: 'Budenovskoye',
    type: 'jv',
    sector: 'Uranium',
    country: 'KZ',
    description: "Uranium mining JV at Budenovskoye deposit (one of the world's largest undeveloped uranium deposits, ~500,000 tU). KZAP holds 51%, Uranium One (Rosatom) 49%. Expected production >6,000 tU/year at full capacity.",
    founded: 2020,
  },

  // Telecoms
  {
    id: 'kztk',
    name: 'Kazakhtelecom',
    shortName: 'KZTK',
    type: 'soe',
    ticker: 'KZTK',
    sector: 'Telecoms',
    country: 'KZ',
    description: 'National incumbent telecom operator. Provides fixed-line, broadband, IPTV, and data center services. Majority state-owned via Samruk-Kazyna. Controls ~75% of Kcell (largest mobile operator) following 2019 acquisition.',
    website: 'telecom.kz',
    founded: 1994,
  },
  {
    id: 'kcell',
    name: 'Kcell',
    shortName: 'KCEL',
    type: 'listed',
    ticker: 'KCEL',
    sector: 'Telecoms',
    country: 'KZ',
    description: "Kazakhstan's largest mobile operator by subscribers (~9 million). Listed on AIX and LSE. Subsidiary of Kazakhtelecom (~75% stake). Provides 4G/5G services across Kazakhstan.",
    founded: 1998,
  },

  // Power grid
  {
    id: 'kegc',
    name: 'KEGOC',
    shortName: 'KEGOC',
    type: 'soe',
    ticker: 'KEGC',
    sector: 'Utilities',
    country: 'KZ',
    description: "National electricity grid operator. Owns and operates Kazakhstan's unified electricity system (UES) — 24,000 km of high-voltage transmission lines (220–500 kV). Monopoly national grid operator; listed on AIX.",
    website: 'kegoc.kz',
    founded: 1997,
  },

  // Aviation
  {
    id: 'aira',
    name: 'Air Astana Group',
    shortName: 'Air Astana',
    type: 'listed',
    ticker: 'AIRA',
    sector: 'Aviation',
    country: 'KZ',
    description: 'National carrier of Kazakhstan. Dual brand: Air Astana (full-service) + FlyArystan (LCC). IPO on AIX and LSE in 2024. Fleet of ~50 aircraft (Boeing 767, A321neo, Embraer). Hub at Almaty and Astana. 51% Samruk-Kazyna, 49% BAE Systems.',
    website: 'airastana.com',
    founded: 2001,
  },

  // Rail
  {
    id: 'ktz',
    name: 'Kazakhstan Temir Zholy',
    shortName: 'KTZ',
    type: 'soe',
    sector: 'Transportation',
    country: 'KZ',
    description: 'National railway holding company. Operates ~16,000 km of rail network, 1,520 mm (Russian broad gauge). Key link in Trans-Caspian International Transport Route (Middle Corridor). Subsidiaries: KTZ Express (logistics), Kaztemirtrans (freight wagons), Trans-Kazakhstan.',
    website: 'railways.kz',
    founded: 1997,
  },

  // Banking / Private
  {
    id: 'hsbk',
    name: 'Halyk Bank',
    shortName: 'HSBK',
    type: 'listed',
    ticker: 'HSBK',
    sector: 'Banking',
    country: 'KZ',
    description: "Kazakhstan's largest bank by assets. Controlled by the Kulibayev-Nazarbayeva family (Timur Kulibayev and Dinara Nazarbayeva own ~52% combined via Almex group). Dominant retail franchise; acquired Kazkommertsbank in 2017. Listed on AIX and LSE.",
    founded: 1923,
  },
  {
    id: 'kspi',
    name: 'Kaspi.kz',
    shortName: 'KSPI',
    type: 'listed',
    ticker: 'KSPI',
    sector: 'Fintech',
    country: 'KZ',
    description: 'Super-app combining payments, marketplace, and banking. ~14 million monthly active users in Kazakhstan. Founders Mikheil Lomtadze (CEO) and Vyacheslav Kim hold ~40% combined. Listed on Nasdaq (KSPI) and AIX. Growing into Central Asia and Azerbaijan.',
    founded: 2008,
  },

  // Foreign strategic partners
  {
    id: 'chevron',
    name: 'Chevron Corporation',
    type: 'foreign',
    country: 'US',
    sector: 'Oil & Gas',
    description: 'American supermajor. Largest foreign investor in Kazakhstan. Holds 50% of TCO (Tengizchevroil) — the largest single foreign investment in post-Soviet Central Asia. Also 18% of CPC pipeline.',
    ticker: 'CVX',
  },
  {
    id: 'exxon',
    name: 'ExxonMobil',
    type: 'foreign',
    country: 'US',
    sector: 'Oil & Gas',
    description: 'American supermajor. Holds 25% of TCO and 16.81% of NCOC (Kashagan).',
    ticker: 'XOM',
  },
  {
    id: 'shell',
    name: 'Shell',
    type: 'foreign',
    country: 'NL',
    sector: 'Oil & Gas',
    description: 'Anglo-Dutch supermajor. Holds 16.81% of NCOC (Kashagan) and 29.25% of KPO (Karachaganak).',
    ticker: 'SHELL',
  },
  {
    id: 'totalenergies',
    name: 'TotalEnergies',
    type: 'foreign',
    country: 'FR',
    sector: 'Oil & Gas',
    description: 'French supermajor. Holds 16.81% of NCOC (Kashagan).',
    ticker: 'TTE',
  },
  {
    id: 'eni',
    name: 'Eni',
    type: 'foreign',
    country: 'IT',
    sector: 'Oil & Gas',
    description: 'Italian supermajor. Holds 16.81% of NCOC (Kashagan) and 29.25% of KPO (Karachaganak).',
    ticker: 'ENI',
  },
  {
    id: 'cnpc',
    name: 'CNPC',
    shortName: 'CNPC',
    type: 'foreign',
    country: 'CN',
    sector: 'Oil & Gas',
    description: 'China National Petroleum Corporation. State-owned. Holds 8.33% of NCOC (Kashagan). Also operates the Kazakhstan-China crude pipeline (AtasU–Alashankou, 988 km, 20 mt/year capacity).',
  },
  {
    id: 'inpex',
    name: 'Inpex',
    type: 'foreign',
    country: 'JP',
    sector: 'Oil & Gas',
    description: "Japan's largest oil & gas company. Holds 7.56% of NCOC (Kashagan).",
    ticker: 'INPX',
  },
  {
    id: 'lukoil',
    name: 'Lukoil',
    type: 'foreign',
    country: 'RU',
    sector: 'Oil & Gas',
    description: 'Russian private oil company. Holds 5% of TCO and 13.5% of KPO (Karachaganak).',
    ticker: 'LKOD',
  },
  {
    id: 'cameco',
    name: 'Cameco Corporation',
    type: 'foreign',
    country: 'CA',
    sector: 'Uranium',
    description: "Canadian uranium producer. World's 2nd largest uranium miner. Holds 40% of JV Inkai in Kazakhstan.",
    ticker: 'CCO',
  },
  {
    id: 'orano',
    name: 'Orano',
    shortName: 'Orano',
    type: 'foreign',
    country: 'FR',
    sector: 'Uranium',
    description: 'French state-owned nuclear fuel cycle company (formerly Areva). Holds 51% of JV Katco in Kazakhstan.',
  },
  {
    id: 'uranium-one',
    name: 'Uranium One',
    type: 'foreign',
    country: 'RU',
    sector: 'Uranium',
    description: "Uranium mining subsidiary of Rosatom (Russia's state nuclear corporation). Holds 49% of JV Budenovskoye.",
  },
  {
    id: 'bae-systems',
    name: 'BAE Systems',
    type: 'foreign',
    country: 'GB',
    sector: 'Aviation',
    description: "British defense and aerospace company. Holds 49% of Air Astana Group; has been a strategic partner since the airline's founding in 2001.",
    ticker: 'BA/',
  },
  {
    id: 'kulibayev-family',
    name: 'Kulibayev–Nazarbayeva Family',
    shortName: 'Kulibayev Family',
    type: 'private',
    country: 'KZ',
    description: 'Timur Kulibayev (son-in-law of former president Nazarbayev) and Dinara Kulibayeva (née Nazarbayeva) control ~52% of Halyk Bank via Almex Holding. Timur Kulibayev chairs the KAZENERGY energy association.',
  },
  {
    id: 'kim-lomtadze',
    name: 'Kim & Lomtadze',
    shortName: 'Founders',
    type: 'private',
    country: 'KZ',
    description: "Vyacheslav Kim and Mikheil Lomtadze co-founded Kaspi.kz and collectively own ~40% of the company. Lomtadze serves as CEO. Together they built Kaspi from a regional bank into Central Asia's dominant super-app.",
  },
]

// ─── Edges ────────────────────────────────────────────────────────────────────

export const EDGES: Edge[] = [
  // Government → funds
  { from: 'gov-kz',  to: 'samruk',        stake: 100, since: 2008 },
  { from: 'gov-kz',  to: 'baiterek',      stake: 100, since: 2013 },

  // Samruk-Kazyna → SOEs
  { from: 'samruk',  to: 'kmg',           stake: 100, since: 2007, note: 'Via National Oil Fund; KMG listed on AIX/LSE Oct 2022 (listed but gov retains 100% economic interest via Samruk)' },
  { from: 'samruk',  to: 'kzap',          stake: 75.04, since: 1997, note: 'Float of 24.96% via IPO Nov 2018 on LSE/AIX' },
  { from: 'samruk',  to: 'kztk',          stake: 51, since: 1994 },
  { from: 'samruk',  to: 'kegc',          stake: 90, since: 1997, note: '10% float on AIX' },
  { from: 'samruk',  to: 'aira',          stake: 51, since: 2001 },
  { from: 'samruk',  to: 'ktz',           stake: 100, since: 1997 },
  { from: 'samruk',  to: 'samruk-energy', stake: 100, since: 2007 },

  // Samruk-Energy → KEGOC (also direct samruk→kegc above)
  // Note: KEGOC is directly under Samruk-Kazyna, not Samruk-Energy

  // KMG tree
  { from: 'kmg',     to: 'kto',           stake: 100, since: 1997 },
  { from: 'kmg',     to: 'ktg',           stake: 100, since: 2000 },
  { from: 'kmg',     to: 'tco',           stake: 20,  since: 1993, note: 'KMG holds 20% in TCO operating consortium' },
  { from: 'kmg',     to: 'ncoc',          stake: 16.88, since: 2008 },
  { from: 'kmg',     to: 'kpo',           stake: 10,  since: 2012, note: 'KMG acquired 10% from BG Group' },

  // Foreign partners in TCO
  { from: 'chevron', to: 'tco',           stake: 50,  since: 1993 },
  { from: 'exxon',   to: 'tco',           stake: 25,  since: 1993 },
  { from: 'lukoil',  to: 'tco',           stake: 5,   since: 1996 },

  // Foreign partners in NCOC (Kashagan)
  { from: 'shell',         to: 'ncoc',    stake: 16.81, since: 2008 },
  { from: 'totalenergies', to: 'ncoc',    stake: 16.81, since: 2008 },
  { from: 'exxon',         to: 'ncoc',    stake: 16.81, since: 2008 },
  { from: 'eni',           to: 'ncoc',    stake: 16.81, since: 2008 },
  { from: 'cnpc',          to: 'ncoc',    stake: 8.33,  since: 2013 },
  { from: 'inpex',         to: 'ncoc',    stake: 7.56,  since: 2008 },

  // Foreign partners in KPO (Karachaganak)
  { from: 'shell', to: 'kpo',             stake: 29.25, since: 1997 },
  { from: 'eni',   to: 'kpo',             stake: 29.25, since: 1997 },
  { from: 'chevron', to: 'kpo',           stake: 18,    since: 1997 },
  { from: 'lukoil',  to: 'kpo',           stake: 13.5,  since: 1997 },

  // Kazatomprom JVs
  { from: 'kzap',    to: 'jv-inkai',       stake: 60, since: 1997 },
  { from: 'cameco',  to: 'jv-inkai',       stake: 40, since: 1997 },

  { from: 'kzap',    to: 'jv-katco',       stake: 49, since: 1997 },
  { from: 'orano',   to: 'jv-katco',       stake: 51, since: 1997 },

  { from: 'kzap',    to: 'jv-budenovskoye', stake: 51, since: 2020 },
  { from: 'uranium-one', to: 'jv-budenovskoye', stake: 49, since: 2020 },

  // Telecoms
  { from: 'kztk',    to: 'kcell',           stake: 75.2, since: 2019, note: 'Kazakhtelecom acquired Kcell from TeliaSonera in 2019 for $445M' },

  // Aviation
  { from: 'bae-systems', to: 'aira',        stake: 49, since: 2001 },

  // Private / family ownership
  { from: 'kulibayev-family', to: 'hsbk',   stake: 52, since: 2005, note: 'Via Almex Holding and related structures' },
  { from: 'kim-lomtadze',     to: 'kspi',   stake: 40, since: 2008 },
]

// ─── Helpers ─────────────────────────────────────────────────────────────────

export const entityMap = Object.fromEntries(ENTITIES.map(e => [e.id, e]))

/** Returns all edges where `id` is the *parent* (owner) */
export function childEdges(id: string): Edge[] {
  return EDGES.filter(e => e.from === id)
}

/** Returns all edges where `id` is the *child* */
export function parentEdges(id: string): Edge[] {
  return EDGES.filter(e => e.to === id)
}

/** Color per entity type */
export const TYPE_COLOR: Record<EntityType, string> = {
  government: '#1d4ed8',   // deep blue
  fund:       '#2563eb',   // blue
  soe:        '#0891b2',   // cyan
  listed:     '#0d9488',   // teal
  jv:         '#d97706',   // amber
  foreign:    '#6b7280',   // gray
  private:    '#7c3aed',   // violet
}

export const TYPE_LABEL: Record<EntityType, string> = {
  government: 'Government',
  fund:       'State Fund',
  soe:        'SOE',
  listed:     'Listed',
  jv:         'JV',
  foreign:    'Foreign',
  private:    'Private',
}
