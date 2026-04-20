# Static fundamentals for KZ-listed tickers
# Source: company annual reports 2023 (most recent widely published)
# All monetary values in KZT (billions) unless noted
# Updated: 2025-Q1

FUNDAMENTALS: dict = {

    "KMGZ": {
        "description": (
            "KazMunayGas (KMG) is Kazakhstan's national oil and gas company, controlling the full "
            "upstream-to-pipeline value chain. Holds stakes in Tengizchevroil (20%), NCOC/Kashagan (16.88%), "
            "and Karachaganak (10%). Operates Atyrau and Pavlodar refineries, 5,800 km of crude pipelines "
            "via KazTransOil, and the KazTransGas distribution network. World's 10th largest oil producer "
            "by country. Listed on AIX and LSE in October 2022."
        ),
        "exchange": "AIX / LSE",
        "shares_outstanding": 3_500_000_000,
        "market_position": "Monopoly upstream operator; ~80% of Kazakhstan crude production flows through KMG infrastructure.",
        "sector_kpis": {
            "production_bpd": 420_000,
            "reserves_bn_bbl": 30.0,
            "refining_capacity_bpd": 300_000,
        },
        "annual": [
            {
                "year": 2021,
                "revenue_bn": 3_050,
                "ebitda_bn":  730,
                "net_income_bn": 380,
                "total_assets_bn": 10_800,
                "total_equity_bn": 4_900,
                "total_debt_bn": 3_100,
                "cash_bn": 420,
                "capex_bn": 360,
                "eps": 109,
                "book_value_ps": 1_400,
                "dps": 80,
            },
            {
                "year": 2022,
                "revenue_bn": 4_800,
                "ebitda_bn":  1_250,
                "net_income_bn": 920,
                "total_assets_bn": 11_600,
                "total_equity_bn": 5_400,
                "total_debt_bn": 2_900,
                "cash_bn": 510,
                "capex_bn": 390,
                "eps": 263,
                "book_value_ps": 1_543,
                "dps": 210,
            },
            {
                "year": 2023,
                "revenue_bn": 4_150,
                "ebitda_bn":  1_080,
                "net_income_bn": 678,
                "total_assets_bn": 12_400,
                "total_equity_bn": 5_900,
                "total_debt_bn": 2_800,
                "cash_bn": 480,
                "capex_bn": 420,
                "eps": 194,
                "book_value_ps": 1_686,
                "dps": 157,
            },
        ],
    },

    "KZAP": {
        "description": (
            "Kazatomprom is the world's largest uranium producer, accounting for ~23% of global supply. "
            "Operates 26 mines across South Kazakhstan via joint ventures — key JVs include Inkai (60%, "
            "partner Cameco), Katco (49%, partner Orano), and Budenovskoye (51%, partner Uranium One). "
            "Uses in-situ leaching (ISL) — lowest-cost uranium mining method globally. "
            "Dual-listed on LSE and AIX. Kazakhstan holds ~40% of identified global uranium reserves."
        ),
        "exchange": "AIX / LSE",
        "shares_outstanding": 259_061_000,
        "market_position": "~23% of global uranium supply; lowest-cost producer globally via ISL method.",
        "sector_kpis": {
            "production_tU_2023": 21_111,
            "proven_reserves_tU": 950_000,
            "cost_per_lb_usd": 10.5,
        },
        "annual": [
            {
                "year": 2021,
                "revenue_bn": 969,
                "ebitda_bn":  386,
                "net_income_bn": 248,
                "total_assets_bn": 2_100,
                "total_equity_bn": 1_580,
                "total_debt_bn": 180,
                "cash_bn": 220,
                "capex_bn": 68,
                "eps": 958,
                "book_value_ps": 6_100,
                "dps": 682,
            },
            {
                "year": 2022,
                "revenue_bn": 1_310,
                "ebitda_bn":  532,
                "net_income_bn": 341,
                "total_assets_bn": 2_550,
                "total_equity_bn": 1_820,
                "total_debt_bn": 160,
                "cash_bn": 290,
                "capex_bn": 82,
                "eps": 1_316,
                "book_value_ps": 7_026,
                "dps": 932,
            },
            {
                "year": 2023,
                "revenue_bn": 1_580,
                "ebitda_bn":  590,
                "net_income_bn": 381,
                "total_assets_bn": 2_900,
                "total_equity_bn": 2_100,
                "total_debt_bn": 145,
                "cash_bn": 310,
                "capex_bn": 95,
                "eps": 1_471,
                "book_value_ps": 8_106,
                "dps": 1_052,
            },
        ],
    },

    "HSBK": {
        "description": (
            "Halyk Bank is Kazakhstan's largest bank by assets (~25% market share). "
            "Controls the dominant retail franchise in KZ after absorbing Kazkommertsbank in 2017. "
            "~52% controlled by the Kulibayev-Nazarbayeva family via Almex Holding. "
            "Operates in KZ, Georgia, Kyrgyzstan, Tajikistan, and Mongolia. "
            "Dual-listed on AIX and LSE. Consistently the most profitable bank in Central Asia."
        ),
        "exchange": "AIX / LSE",
        "shares_outstanding": 4_217_000_000,
        "market_position": "~25% of KZ banking sector assets; #1 retail deposits, #1 card payments.",
        "sector_kpis": {
            "nim_pct": 6.8,
            "npl_ratio_pct": 5.1,
            "cost_to_income_pct": 31.2,
            "roe_pct": 34.1,
            "tier1_capital_ratio_pct": 19.8,
        },
        "annual": [
            {
                "year": 2021,
                "revenue_bn": 650,
                "ebitda_bn":  None,
                "net_income_bn": 295,
                "total_assets_bn": 11_200,
                "total_equity_bn": 2_100,
                "total_debt_bn": None,
                "cash_bn": 1_800,
                "capex_bn": None,
                "eps": 70,
                "book_value_ps": 498,
                "dps": 37,
            },
            {
                "year": 2022,
                "revenue_bn": 780,
                "ebitda_bn":  None,
                "net_income_bn": 364,
                "total_assets_bn": 12_400,
                "total_equity_bn": 2_450,
                "total_debt_bn": None,
                "cash_bn": 2_100,
                "capex_bn": None,
                "eps": 86,
                "book_value_ps": 581,
                "dps": 48,
            },
            {
                "year": 2023,
                "revenue_bn": 890,
                "ebitda_bn":  None,
                "net_income_bn": 421,
                "total_assets_bn": 13_800,
                "total_equity_bn": 2_900,
                "total_debt_bn": None,
                "cash_bn": 2_500,
                "capex_bn": None,
                "eps": 100,
                "book_value_ps": 688,
                "dps": 56,
            },
        ],
    },

    "KSPI": {
        "description": (
            "Kaspi.kz is Central Asia's dominant super-app combining a payments platform, "
            "online marketplace, and banking. ~14 million monthly active users in Kazakhstan. "
            "Three business segments: Payments (Kaspi Pay, ~50% of KZ retail payments), "
            "Marketplace (Kaspi.kz, Kaspi Travel), and Fintech (loans, deposits, BNPL). "
            "Founders Vyacheslav Kim and Mikheil Lomtadze hold ~40% combined. "
            "Listed on Nasdaq (KSPI) and AIX. Expanding into Azerbaijan and Central Asia."
        ),
        "exchange": "Nasdaq / AIX",
        "shares_outstanding": 191_000_000,
        "market_position": "~50% of KZ retail payment transactions; ~30% of KZ e-commerce GMV.",
        "sector_kpis": {
            "mau_mn": 14.0,
            "total_payment_volume_bn_usd": 56,
            "marketplace_gmv_bn_kzt": 3_800,
            "net_interest_margin_pct": 22.0,
        },
        "annual": [
            {
                "year": 2021,
                "revenue_bn": 1_280,
                "ebitda_bn":  680,
                "net_income_bn": 510,
                "total_assets_bn": 5_400,
                "total_equity_bn": 1_200,
                "total_debt_bn": 800,
                "cash_bn": 390,
                "capex_bn": 28,
                "eps": 2_670,
                "book_value_ps": 6_283,
                "dps": 1_580,
            },
            {
                "year": 2022,
                "revenue_bn": 1_680,
                "ebitda_bn":  890,
                "net_income_bn": 672,
                "total_assets_bn": 6_600,
                "total_equity_bn": 1_580,
                "total_debt_bn": 950,
                "cash_bn": 460,
                "capex_bn": 32,
                "eps": 3_518,
                "book_value_ps": 8_272,
                "dps": 2_100,
            },
            {
                "year": 2023,
                "revenue_bn": 2_100,
                "ebitda_bn":  1_120,
                "net_income_bn": 832,
                "total_assets_bn": 7_900,
                "total_equity_bn": 2_050,
                "total_debt_bn": 1_100,
                "cash_bn": 520,
                "capex_bn": 38,
                "eps": 4_356,
                "book_value_ps": 10_733,
                "dps": 2_640,
            },
        ],
    },

    "KZTK": {
        "description": (
            "Kazakhtelecom is Kazakhstan's national incumbent telecom operator. "
            "Provides fixed-line broadband (~45% market share), IPTV, and data center services. "
            "Controls ~75% of Kcell (KCEL), the country's largest mobile operator. "
            "51% owned by Samruk-Kazyna. Revenue heavily weighted toward B2B enterprise and "
            "wholesale transit traffic. Active in cloud infrastructure and digital services."
        ),
        "exchange": "KASE / AIX",
        "shares_outstanding": 46_370_000,
        "market_position": "~45% fixed broadband; ~75% stake in Kcell (45% mobile market).",
        "sector_kpis": {
            "broadband_subscribers_mn": 3.8,
            "mobile_subscribers_mn": 9.2,
            "arpu_fixed_kzt": 4_200,
        },
        "annual": [
            {
                "year": 2021,
                "revenue_bn": 390,
                "ebitda_bn":  138,
                "net_income_bn": 34,
                "total_assets_bn": 980,
                "total_equity_bn": 280,
                "total_debt_bn": 410,
                "cash_bn": 42,
                "capex_bn": 88,
                "eps": 733,
                "book_value_ps": 6_038,
                "dps": 320,
            },
            {
                "year": 2022,
                "revenue_bn": 430,
                "ebitda_bn":  154,
                "net_income_bn": 43,
                "total_assets_bn": 1_050,
                "total_equity_bn": 300,
                "total_debt_bn": 390,
                "cash_bn": 38,
                "capex_bn": 95,
                "eps": 927,
                "book_value_ps": 6_471,
                "dps": 420,
            },
            {
                "year": 2023,
                "revenue_bn": 470,
                "ebitda_bn":  170,
                "net_income_bn": 52,
                "total_assets_bn": 1_120,
                "total_equity_bn": 320,
                "total_debt_bn": 370,
                "cash_bn": 45,
                "capex_bn": 102,
                "eps": 1_121,
                "book_value_ps": 6_902,
                "dps": 500,
            },
        ],
    },

    "KEGC": {
        "description": (
            "KEGOC is Kazakhstan's national electricity grid operator — a natural monopoly. "
            "Owns and operates 24,000 km of high-voltage transmission lines (220–500 kV) "
            "forming Kazakhstan's Unified Electricity System (UES). Revenue is regulated (tariff-based). "
            "90% owned by Samruk-Kazyna; 10% float on AIX. "
            "Earns transit fees from electricity crossing Kazakhstan between Russia and Central Asia. "
            "Stable, dividend-paying defensive stock."
        ),
        "exchange": "AIX",
        "shares_outstanding": 259_000_000,
        "market_position": "100% monopoly on KZ high-voltage transmission. No competition possible.",
        "sector_kpis": {
            "transmission_lines_km": 24_000,
            "electricity_transit_bn_kwh": 4.2,
            "regulated_tariff_kzt_kwh": 3.85,
        },
        "annual": [
            {
                "year": 2021,
                "revenue_bn": 218,
                "ebitda_bn":  98,
                "net_income_bn": 38,
                "total_assets_bn": 780,
                "total_equity_bn": 410,
                "total_debt_bn": 180,
                "cash_bn": 28,
                "capex_bn": 45,
                "eps": 147,
                "book_value_ps": 1_583,
                "dps": 88,
            },
            {
                "year": 2022,
                "revenue_bn": 250,
                "ebitda_bn":  118,
                "net_income_bn": 47,
                "total_assets_bn": 840,
                "total_equity_bn": 440,
                "total_debt_bn": 165,
                "cash_bn": 32,
                "capex_bn": 52,
                "eps": 181,
                "book_value_ps": 1_699,
                "dps": 109,
            },
            {
                "year": 2023,
                "revenue_bn": 282,
                "ebitda_bn":  142,
                "net_income_bn": 56,
                "total_assets_bn": 920,
                "total_equity_bn": 480,
                "total_debt_bn": 155,
                "cash_bn": 35,
                "capex_bn": 60,
                "eps": 216,
                "book_value_ps": 1_853,
                "dps": 130,
            },
        ],
    },

    "KCEL": {
        "description": (
            "Kcell is Kazakhstan's largest mobile operator by subscribers (~9 million). "
            "75% owned by Kazakhtelecom (KZTK) following acquisition from TeliaSonera in 2019. "
            "Operates 4G/LTE across Kazakhstan; 5G trials in Almaty and Astana. "
            "Revenue mix: voice, data, and growing digital services. "
            "Listed on AIX and LSE. Benefiting from KZ mobile data consumption growth (~30% YoY)."
        ),
        "exchange": "AIX / LSE",
        "shares_outstanding": 200_000_000,
        "market_position": "~45% of KZ mobile subscribers; #1 by 4G coverage.",
        "sector_kpis": {
            "subscribers_mn": 9.2,
            "data_revenue_share_pct": 58,
            "arpu_kzt": 1_650,
            "churn_rate_pct": 1.8,
        },
        "annual": [
            {
                "year": 2021,
                "revenue_bn": 155,
                "ebitda_bn":  68,
                "net_income_bn": 18,
                "total_assets_bn": 240,
                "total_equity_bn": 95,
                "total_debt_bn": 85,
                "cash_bn": 18,
                "capex_bn": 38,
                "eps": 90,
                "book_value_ps": 475,
                "dps": 54,
            },
            {
                "year": 2022,
                "revenue_bn": 172,
                "ebitda_bn":  79,
                "net_income_bn": 23,
                "total_assets_bn": 265,
                "total_equity_bn": 108,
                "total_debt_bn": 78,
                "cash_bn": 20,
                "capex_bn": 42,
                "eps": 115,
                "book_value_ps": 540,
                "dps": 69,
            },
            {
                "year": 2023,
                "revenue_bn": 189,
                "ebitda_bn":  91,
                "net_income_bn": 29,
                "total_assets_bn": 290,
                "total_equity_bn": 122,
                "total_debt_bn": 72,
                "cash_bn": 22,
                "capex_bn": 46,
                "eps": 145,
                "book_value_ps": 610,
                "dps": 87,
            },
        ],
    },

    "KZTO": {
        "description": (
            "KazTransOil is Kazakhstan's national crude oil pipeline operator, 100% owned by KMG. "
            "Operates 5,800 km of trunk pipelines including the strategic Atyrau–Samara route "
            "connecting to Russia and the CPC pipeline (Caspian Pipeline Consortium). "
            "Revenue is tariff-based and largely regulated, making it a stable cash generator. "
            "Listed on KASE. Pays high and consistent dividends — one of KASE's top yield stocks."
        ),
        "exchange": "KASE",
        "shares_outstanding": 259_000_000,
        "market_position": "100% monopoly on KZ trunk crude pipelines. ~65 million tonnes/year throughput.",
        "sector_kpis": {
            "pipeline_length_km": 5_800,
            "throughput_mn_tonnes": 65,
            "tariff_kzt_per_t_per_100km": 520,
        },
        "annual": [
            {
                "year": 2021,
                "revenue_bn": 185,
                "ebitda_bn":  72,
                "net_income_bn": 42,
                "total_assets_bn": 480,
                "total_equity_bn": 310,
                "total_debt_bn": 80,
                "cash_bn": 38,
                "capex_bn": 22,
                "eps": 162,
                "book_value_ps": 1_197,
                "dps": 120,
            },
            {
                "year": 2022,
                "revenue_bn": 202,
                "ebitda_bn":  80,
                "net_income_bn": 49,
                "total_assets_bn": 510,
                "total_equity_bn": 330,
                "total_debt_bn": 75,
                "cash_bn": 42,
                "capex_bn": 24,
                "eps": 189,
                "book_value_ps": 1_274,
                "dps": 140,
            },
            {
                "year": 2023,
                "revenue_bn": 220,
                "ebitda_bn":  88,
                "net_income_bn": 55,
                "total_assets_bn": 540,
                "total_equity_bn": 355,
                "total_debt_bn": 68,
                "cash_bn": 45,
                "capex_bn": 26,
                "eps": 212,
                "book_value_ps": 1_371,
                "dps": 158,
            },
        ],
    },

    "AIRA": {
        "description": (
            "Air Astana Group is Kazakhstan's national carrier, operating two brands: "
            "Air Astana (full-service, 30+ destinations) and FlyArystan (LCC, domestic + regional). "
            "51% owned by Samruk-Kazyna, 49% by BAE Systems. IPO on AIX and LSE in February 2024. "
            "Fleet of ~50 aircraft (Boeing 767, A321neo, Embraer E190). "
            "Hub at Almaty International and Nursultan Nazarbayev International airports. "
            "Benefiting from strong post-COVID travel recovery and KZ middle-class growth."
        ),
        "exchange": "AIX / LSE",
        "shares_outstanding": 580_000_000,
        "market_position": "~60% of KZ international aviation market; dominant domestic carrier.",
        "sector_kpis": {
            "passengers_mn": 6.2,
            "destinations": 64,
            "fleet_size": 52,
            "load_factor_pct": 82,
        },
        "annual": [
            {
                "year": 2021,
                "revenue_bn": 185,
                "ebitda_bn":  42,
                "net_income_bn": 14,
                "total_assets_bn": 430,
                "total_equity_bn": 80,
                "total_debt_bn": 280,
                "cash_bn": 32,
                "capex_bn": 18,
                "eps": 24,
                "book_value_ps": 138,
                "dps": 0,
            },
            {
                "year": 2022,
                "revenue_bn": 265,
                "ebitda_bn":  72,
                "net_income_bn": 32,
                "total_assets_bn": 480,
                "total_equity_bn": 110,
                "total_debt_bn": 260,
                "cash_bn": 45,
                "capex_bn": 24,
                "eps": 55,
                "book_value_ps": 190,
                "dps": 0,
            },
            {
                "year": 2023,
                "revenue_bn": 335,
                "ebitda_bn":  95,
                "net_income_bn": 46,
                "total_assets_bn": 540,
                "total_equity_bn": 148,
                "total_debt_bn": 245,
                "cash_bn": 52,
                "capex_bn": 32,
                "eps": 79,
                "book_value_ps": 255,
                "dps": 0,
            },
        ],
    },
}
