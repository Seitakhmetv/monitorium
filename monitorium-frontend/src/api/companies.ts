import get from './client'

export interface Company {
  ticker: string
  shortName: string
  sector: string
  industry: string
  country: string
  marketCap: number
  currency: string
}

export const getCompanies = () => get<Company[]>('/companies')
export const getCompany = (ticker: string) => get<Company>(`/companies/${ticker}`)
