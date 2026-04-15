import get from './client'

export interface MacroPoint {
  indicator_name: string
  country_code: string
  year: number
  value: number
}

export const getMacro = (params: {
  indicator?: string
  country?: string
  from?: number
  to?: number
}) => get<MacroPoint[]>('/macro', params)

export const getMacroIndicators = () => get<string[]>('/macro/indicators')
