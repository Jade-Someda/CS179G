export const TABLE_META = {
  airport_theft_count_comparison: { //jade
    question: 'How does Airport theft rates compare to other places?',
    hypothesis: ' More theft incidents occur around airports compared to other areas.',
    description: 'Practical for understanding the safety-levels around Airports.',
    category: 'Location-Based/ Spatial',
  },
  crimes_by_location:{ //jade
    question: 'Which locations correlate highly with a specific type of crime?',
    description: ' The table below illustrates compares the primary crime to the total amount of crime per location, to provide the relative context.',
    category: 'Location-Based/ Spatial',
  },
  crimes_by_location_and_type:{ //jade
    question: 'What are the main crimes for each location?',
    description: 'Understand the correlation between location and crime.',
    category: 'Location-Based/ Spatial',
  },
  downtown_vs_residential_theft_robbery: //jade
  {
    question: 'Downtown vs Residential',
    hypothesis: 'Downtown areas have higher rates of theft and robbery than residential areas.',
    description: 'Comparison of crime rates between the Downtown and Residential Areas.',
    category: 'Location-Based/ Spatial',
  },
  sport_location_crimes:{ //jade
    question: 'Sports Location Crimes',
    hypothesis: 'Assault and battery incidents are the most common crime during major sporting events',
    description: 'The data below illustrates what incidents are most common at sporting areas/events.',
    category:'Location-Based/ Spatial'
  },
  theft_by_location: { //jade
    question:'What are the crimes and theft rates in different locations?', //not sure of original question?
    description: 'ADD DESCRIPTION PLS', //ADD DESCRIPTION
    category: 'Location-Based/ Spatial'
  },
  transit_vs_commercial_robbery_count://jade
  { 
    question:'Transit v.s Commercial',
    hypothesis:'Public transit locations have higher robbery rates than commercial areas.', //not sure of original question?
    description: 'Transit locations include but are not limited to: Bus, Trains, Stations, etc. Commercial areas include but are not limited to: Stores, Shop, Market, etc. ', //ADD DESCRIPTION
    category: 'Location-Based/ Spatial'
  },

  hourly_crimes: {
    question: 'How does crime volume change by hour of day?',
    hypothesis: 'Evening and overnight hours see higher crime than daytime.',
    description: 'Each bar represents the number of reported incidents for a particular hour across the city.',
    category: 'Time of day',
  },
  time_period_crimes: {
    question: 'Which time of day is riskiest overall?',
    hypothesis: 'Crimes cluster in evening and overnight periods.',
    description: 'Daytime, evening, and overnight windows aggregated across all years in the dataset.',
    category: 'Time of day',
  },
  monthly_crimes: {
    question: 'How does crime vary across the calendar year?',
    hypothesis: 'Summer months experience a noticeable increase in crime volume.',
    description: 'Total crimes per month across years, useful for seasonal patterns.',
    category: 'Seasonality',
  },
  yearly_crimes: {
    question: 'How have crime levels changed over the years?',
    hypothesis: 'Crime rates exhibit clear long-term trends between 2001 and 2025.',
    description: 'Annual totals across all crime types, useful for spotting long-term shifts.',
    category: 'Long-term trends',
  },
  community_area_crimes: {
    question: 'Which community areas see the most reported crime?',
    hypothesis: 'Downtown and dense neighborhoods appear as persistent hotspots.',
    description: 'Crimes aggregated by community area; pairs well with map visualizations.',
    category: 'Location & spatial patterns',
  },
  thanksgiving_by_type: {
    question: 'What crime types rise during Thanksgiving?',
    hypothesis: 'Certain crime types spike during the Thanksgiving holiday period.',
    description: 'Crimes by type during Thanksgiving period, showing which offenses are most common.',
    category: 'Seasonality',
  },


}

export const COMMUNITY_AREA_COORDS = {
  '8': [41.9009, -87.6265],
  '24': [41.8736, -87.6477],
  '25': [41.8607, -87.6252],
  '28': [41.8811, -87.7642],
  '29': [41.8555, -87.7152],
  '31': [41.7908, -87.7294],
  '32': [41.8781, -87.6298],
  '33': [41.8696, -87.6872],
  '35': [41.8309, -87.6847],
  '38': [41.778, -87.7677],
  '43': [41.8089, -87.7246],
  '44': [41.7474, -87.6638],
  '67': [41.7748, -87.6801],
  '68': [41.7757, -87.6045],
  '71': [41.7441, -87.6476],
  '76': [41.9437, -87.6553],
}

export const INSIGHT_STATUS_META = {
  supported: {
    label: 'Hypothesis supported',
    className: 'insight-badge insight-badge--supported',
  },
  mixed: {
    label: 'Hypothesis partially supported',
    className: 'insight-badge insight-badge--mixed',
  },
  not_supported: {
    label: 'Hypothesis not supported',
    className: 'insight-badge insight-badge--not-supported',
  },
}
