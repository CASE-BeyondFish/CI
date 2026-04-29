import { DataSourceConfig } from './types';

const currentYear = new Date().getFullYear();

export const DATA_SOURCES: DataSourceConfig[] = [
  {
    id: 'adm',
    label: 'Actuarial Data Master',
    description: 'Rates, prices, T-yields, coverage levels — the core insurance data',
    baseUrl: 'https://pubfs-rma.fpac.usda.gov/pub/References/actuarial_data_master',
    listingStrategy: 'year-based',
    yearRange: { start: 2020, end: currentYear + 1 },
    localDir: 'adm',
    filePattern: /\.zip$/i,
  },
  {
    id: 'special_provisions',
    label: 'Special Provisions',
    description: 'Sales closing dates, planting dates, county-level overrides',
    baseUrl: 'https://pubfs-rma.fpac.usda.gov/pub/Special_Provisions',
    listingStrategy: 'year-based',
    yearRange: { start: 2020, end: currentYear + 1 },
    localDir: 'special_provisions',
    filePattern: /\.zip$/i,
  },
  {
    id: 'sob',
    label: 'Summary of Business',
    description: 'Premiums, indemnities, participation, loss ratios by state/county/crop',
    baseUrl: 'https://pubfs-rma.fpac.usda.gov/pub/Web_Data_Files/Summary_of_Business/state_county_crop',
    listingStrategy: 'flat',
    localDir: 'sob',
    filePattern: /\.(zip)$/i,
  },
  {
    id: 'cause_of_loss',
    label: 'Cause of Loss',
    description: 'Claims broken down by peril (drought, excess moisture, etc.)',
    baseUrl: 'https://pubfs-rma.fpac.usda.gov/pub/Web_Data_Files/Summary_of_Business/cause_of_loss',
    listingStrategy: 'flat',
    localDir: 'cause_of_loss',
    filePattern: /\.(zip)$/i,
  },
];

export function getSourceConfig(id: string): DataSourceConfig | undefined {
  return DATA_SOURCES.find((s) => s.id === id);
}
