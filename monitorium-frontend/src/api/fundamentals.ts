import get from './client'

export interface StatementYear {
  fiscal_year:            number
  quarter:                number | null
  currency:               string
  units:                  string
  revenue:                number | null
  gross_profit:           number | null
  operating_profit:       number | null
  ebitda:                 number | null
  net_income:             number | null
  eps:                    number | null
  total_assets:           number | null
  total_liabilities:      number | null
  total_equity:           number | null
  total_debt:             number | null
  cash_and_equivalents:   number | null
  current_assets:         number | null
  current_liabilities:    number | null
  operating_cash_flow:    number | null
  investing_cash_flow:    number | null
  financing_cash_flow:    number | null
  capex:                  number | null
  free_cash_flow:         number | null
  net_interest_income:    number | null
  loan_portfolio:         number | null
  deposits:               number | null
  npl_ratio:              number | null
  capital_adequacy_ratio: number | null
}

export interface Statements {
  ticker:  string
  source:  'bq' | 'static'
  years:   StatementYear[]
}

export interface SankeyNode { id: string }
export interface SankeyLink { source: string; target: string; value: number }

export interface FlowData {
  ticker:   string
  year:     number
  currency: string
  units:    string
  nodes:    SankeyNode[]
  links:    SankeyLink[]
}

export const getStatements = async (ticker: string): Promise<Statements> => {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const data = await get<any>(`/fundamentals/${ticker}/statements`)
  if (data.source === 'static') {
    // Static fallback uses _bn fields + `year` instead of `fiscal_year`
    // Normalise to StatementYear shape with units='billions' so fmtVal works
    const years: StatementYear[] = data.years.map((y: any) => ({
      fiscal_year:            y.year,
      quarter:                null,
      currency:               'KZT',
      units:                  'billions',
      revenue:                y.revenue_bn          ?? null,
      gross_profit:           null,
      operating_profit:       null,
      ebitda:                 y.ebitda_bn           ?? null,
      net_income:             y.net_income_bn       ?? null,
      eps:                    y.eps                 ?? null,
      total_assets:           y.total_assets_bn     ?? null,
      total_liabilities:      y.total_liabilities_bn ?? null,
      total_equity:           y.total_equity_bn     ?? null,
      total_debt:             y.total_debt_bn       ?? null,
      cash_and_equivalents:   y.cash_bn             ?? null,
      current_assets:         null,
      current_liabilities:    null,
      operating_cash_flow:    null,
      investing_cash_flow:    null,
      financing_cash_flow:    null,
      capex:                  y.capex_bn            ?? null,
      free_cash_flow:         null,
      net_interest_income:    null,
      loan_portfolio:         null,
      deposits:               null,
      npl_ratio:              null,
      capital_adequacy_ratio: null,
    }))
    return { ticker: data.ticker, source: 'static', years }
  }
  return data as Statements
}
export const getFlows        = (ticker: string, year: number) => get<FlowData>(`/fundamentals/${ticker}/flows/${year}`)
export const getFlowsYears   = (ticker: string)              => get<number[]>(`/fundamentals/${ticker}/flows`)
export const getFinTickers   = ()                            => get<string[]>('/fundamentals/tickers')

// ── getFundamentals — client-side adapter over getStatements ──────────────────

function toBn(value: number | null | undefined, units: string): number {
  if (value == null) return 0
  switch (units.toLowerCase()) {
    case 'billions':  return value
    case 'millions':  return value / 1_000
    case 'thousands': return value / 1_000_000
    default:          return value / 1_000_000_000
  }
}

export interface FundamentalsYear {
  year:            number
  revenue_bn:      number
  ebitda_bn:       number
  net_income_bn:   number
  eps:             number
  dps:             number
  book_value_ps:   number
  capex_bn:        number
  total_assets_bn: number
  total_equity_bn: number
  total_debt_bn:   number
  cash_bn:         number
}

export interface Fundamentals {
  ticker:             string
  exchange:           string | null
  description:        string | null
  market_position:    string | null
  shares_outstanding: number | null
  sector_kpis:        Record<string, number | string>
  annual:             FundamentalsYear[]
}

export const getFundamentals = async (ticker: string): Promise<Fundamentals> => {
  const stmts = await getStatements(ticker)
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const raw = stmts.years as any[]

  const annual: FundamentalsYear[] = raw.map(y => {
    if (stmts.source === 'static') {
      // API already returns _bn fields + dps/book_value_ps
      return {
        year:            y.year,
        revenue_bn:      y.revenue_bn      ?? 0,
        ebitda_bn:       y.ebitda_bn       ?? 0,
        net_income_bn:   y.net_income_bn   ?? 0,
        eps:             y.eps             ?? 0,
        dps:             y.dps             ?? 0,
        book_value_ps:   y.book_value_ps   ?? 0,
        capex_bn:        y.capex_bn        ?? 0,
        total_assets_bn: y.total_assets_bn ?? 0,
        total_equity_bn: y.total_equity_bn ?? 0,
        total_debt_bn:   y.total_debt_bn   ?? 0,
        cash_bn:         y.cash_bn         ?? 0,
      }
    }
    // bq source: raw values with units
    const u = y.units ?? 'millions'
    return {
      year:            y.fiscal_year,
      revenue_bn:      toBn(y.revenue,             u),
      ebitda_bn:       toBn(y.ebitda,              u),
      net_income_bn:   toBn(y.net_income,           u),
      eps:             y.eps            ?? 0,
      dps:             0,
      book_value_ps:   0,
      capex_bn:        toBn(y.capex,               u),
      total_assets_bn: toBn(y.total_assets,         u),
      total_equity_bn: toBn(y.total_equity,         u),
      total_debt_bn:   toBn(y.total_debt,           u),
      cash_bn:         toBn(y.cash_and_equivalents, u),
    }
  })
  return {
    ticker,
    exchange:           null,
    description:        null,
    market_position:    null,
    shares_outstanding: null,
    sector_kpis:        {},
    annual,
  }
}
