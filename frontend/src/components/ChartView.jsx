import {
  Bar,
  BarChart,
  CartesianGrid,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'
import { CircleMarker, MapContainer, TileLayer, Tooltip as MapTooltip } from 'react-leaflet'
import 'leaflet/dist/leaflet.css'
import { COMMUNITY_AREA_COORDS } from '../constants/tables'

export default function ChartView({ table, data }) {
  if (!Array.isArray(data) || data.length === 0) {
    return <p className="chart-empty">No chart data available for this selection.</p>
  }

  if (table === 'hourly_crimes') {
    const rows = [...data]
      .map(row => ({
        ...row,
        hour_num: Number(row.hour),
        crime_count_num: Number(row.crime_count),
      }))
      .filter(row => Number.isFinite(row.hour_num) && Number.isFinite(row.crime_count_num))
      .sort((a, b) => a.hour_num - b.hour_num)

    return (
      <div className="chart-block">
        <div className="chart-title">Crime volume by hour of day</div>
        <div className="chart-wrap">
          <ResponsiveContainer width="100%" height={290}>
            <BarChart data={rows}>
              <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
              <XAxis dataKey="hour_num" label={{ value: 'Hour of Day', position: 'insideBottom', offset: -5 }} />
              <YAxis tick={{ fontSize: 12 }} />
              <Tooltip />
              <Bar dataKey="crime_count_num" fill="#10b981" radius={[6, 6, 0, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </div>
      </div>
    )
  }

  if (table === 'yearly_crimes') {
    const rows = [...data]
      .map(row => ({
        ...row,
        year_num: Number(row.year),
        total_crimes_num: Number(row.total_crimes),
      }))
      .filter(row => Number.isFinite(row.year_num) && Number.isFinite(row.total_crimes_num))
      .sort((a, b) => a.year_num - b.year_num)

    return (
      <div className="chart-block">
        <div className="chart-title">Yearly crime trend</div>
        <div className="chart-wrap">
          <ResponsiveContainer width="100%" height={290}>
            <LineChart data={rows}>
              <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
              <XAxis dataKey="year_num" />
              <YAxis />
              <Tooltip />
              <Line
                type="monotone"
                dataKey="total_crimes_num"
                stroke="#2563eb"
                strokeWidth={2.5}
                dot={{ r: 2.5 }}
              />
            </LineChart>
          </ResponsiveContainer>
        </div>
      </div>
    )
  }

  if (table === 'time_period_crimes') {
    const rows = [...data]
      .map(row => ({
        ...row,
        time_period: String(row.time_period || 'Unknown'),
        total_crimes_num: Number(row.total_crimes),
      }))
      .filter(row => Number.isFinite(row.total_crimes_num))

    return (
      <div className="chart-block">
        <div className="chart-title">Crime risk by time of day</div>
        <div className="chart-wrap">
          <ResponsiveContainer width="100%" height={290}>
            <BarChart data={rows}>
              <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
              <XAxis dataKey="time_period" />
              <YAxis tick={{ fontSize: 12 }} />
              <Tooltip />
              <Bar dataKey="total_crimes_num" fill="#3b82f6" radius={[6, 6, 0, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </div>
      </div>
    )
  }

 if (table === 'holiday_vs_nonholiday') {
  // Total number of years in the dataset
  const NUM_YEARS = 20;
  const HOLIDAYS_PER_YEAR = 11;
  const DAYS_PER_YEAR = 365;

  // Total days over all years
  const totalHolidayDays = HOLIDAYS_PER_YEAR * NUM_YEARS;
  const totalNonHolidayDays = (DAYS_PER_YEAR - HOLIDAYS_PER_YEAR) * NUM_YEARS;

  const rows = [...data]
    .map(row => {
      const totalCrimes = Number(row.total_crimes);
      let avg_crimes_per_day = totalCrimes; // fallback
      if (row.day_type === "Holiday") {
        avg_crimes_per_day = totalCrimes / totalHolidayDays;
      } else if (row.day_type === "Non-Holiday") {
        avg_crimes_per_day = totalCrimes / totalNonHolidayDays;
      }
      return {
        day_type: row.day_type,
        avg_crimes_per_day
      };
    })
    .filter(row => Number.isFinite(row.avg_crimes_per_day));

  return (
    <div className="chart-block">
      <div className="chart-title">Holiday vs Non-Holiday Crimes (Average per Day)</div>
      <div className="chart-wrap">
        <ResponsiveContainer width="100%" height={290}>
          <BarChart data={rows}>
            <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
            <XAxis dataKey="day_type" />
            <YAxis tick={{ fontSize: 12 }} />
            <Tooltip formatter={(v) => v.toFixed(2)} />
            <Bar dataKey="avg_crimes_per_day" fill="#f43f5e" radius={[6, 6, 0, 0]} />
          </BarChart>
        </ResponsiveContainer>
      </div>
    </div>
  )
}


  if (table === 'community_area_crimes') {
    const topRows = [...data]
      .map(row => ({
        ...row,
        community_area_text: String(row.community_area),
        total_crimes_num: Number(row.total_crimes),
      }))
      .filter(row => Number.isFinite(row.total_crimes_num))
      .sort((a, b) => b.total_crimes_num - a.total_crimes_num)
      .slice(0, 15)

    const mapRows = topRows.filter(row => COMMUNITY_AREA_COORDS[row.community_area_text])
    const maxCrimes = Math.max(...topRows.map(row => row.total_crimes_num), 1)

    return (
      <div className="chart-block">
        <div className="chart-title">Top community areas by reported crime</div>
        <div className="chart-wrap">
          <ResponsiveContainer width="100%" height={290}>
            <BarChart data={topRows}>
              <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
              <XAxis dataKey="community_area_text" />
              <YAxis />
              <Tooltip />
              <Bar dataKey="total_crimes_num" fill="#2563eb" radius={[6, 6, 0, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </div>
        <div className="map-wrap">
          <MapContainer center={[41.8781, -87.6298]} zoom={10} className="crime-map">
            <TileLayer
              attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
              url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
            />
            {mapRows.map(row => {
              const [lat, lng] = COMMUNITY_AREA_COORDS[row.community_area_text]
              const scaled = Math.sqrt(row.total_crimes_num / maxCrimes)
              const radius = Math.max(6, Math.min(20, 6 + scaled * 14))
              return (
                <CircleMarker
                  key={row.community_area_text}
                  center={[lat, lng]}
                  radius={radius}
                  pathOptions={{ color: '#1d4ed8', fillColor: '#60a5fa', fillOpacity: 0.52 }}
                >
                  <MapTooltip>
                    Community Area {row.community_area_text}: {row.total_crimes_num} crimes
                  </MapTooltip>
                </CircleMarker>
              )
            })}
          </MapContainer>
        </div>
      </div>
    )
  }

  if (table === 'thanksgiving_by_type') {
    const rows = [...data]
      .map(row => ({
        ...row,
        primary_type: String(row.primary_type || 'Unknown'),
        total_num: Number(row.total),
      }))
      .filter(row => Number.isFinite(row.total_num))
      .sort((a, b) => b.total_num - a.total_num)
      .slice(0, 5)

    return (
      <div className="chart-block">
        <div className="chart-title">Thanksgiving crimes by type</div>
        <div className="chart-wrap">
          <ResponsiveContainer width="100%" height={290}>
            <BarChart data={rows}>
              <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
              <XAxis dataKey="primary_type" angle={0} textAnchor="end" height={100} />
              <YAxis tick={{ fontSize: 12 }} />
              <Tooltip />
              <Bar dataKey="total_num" fill="#f59e0b" radius={[6, 6, 0, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </div>
      </div>
    )
  }

  const renderWrappedTick = ({ x, y, payload }) => {
    const words = payload.value.split(" ")
  
    return (
      <g transform={`translate(${x},${y})`}>
        <text
          x={0}
          y={0}
          dy={16}
          textAnchor="middle"
          fill="#666"
          fontSize={10}
        >
          {words.map((word, i) => (
            <tspan key={i} x="0" dy={i === 0 ? 0 : 12}>
              {word}
            </tspan>
          ))}
        </text>
      </g>
    )
  }
  
  if (table === 'theft_by_location') {
    const rows = [...data]
      .map(row => ({
        location: String(row.location_description || 'Unknown'),
        total_crimes: Number(row.total_crimes),
        total_thefts: Number(row.total_thefts),
        theft_rate: Number(row.total_thefts) / Number(row.total_crimes),
      }))
      .filter(row => Number.isFinite(row.total_crimes) && row.total_crimes > 0)
      .sort((a, b) => b.theft_rate - a.theft_rate)
      .slice(0, 10)
  
    return (
      <div className="chart-block">
        <div className="chart-title">Theft rate by location</div>
  
        <div className="chart-wrap">
          <ResponsiveContainer width="100%" height={320}>
            <BarChart
              layout="vertical"
              data={rows}
              margin={{ top: 10, right: -40, left: -65, bottom: 0 }}
            >
              <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
  
              <XAxis type="number" domain={[0, 1]} />
  
              <YAxis
                type="category"
                dataKey="location"
                width={260}
                tick={{ fontSize: 11 }}
              />
              
  
              <Tooltip
                formatter={(value, name, props) => {
                  const { total_thefts, total_crimes } = props.payload
                  return [
                    `${(value * 100).toFixed(2)}%`,
                    `Theft Rate (${total_thefts}/${total_crimes})`
                  ]
                }}
              />
  
              <Bar
                dataKey="theft_rate"
                fill="#f59e0b"
                radius={[0, 6, 6, 0]}
              />
            </BarChart>
          </ResponsiveContainer>
        </div>
      </div>
    )
  }
  // if(table === transit_vs_commercial_robbery_count) {      finish, include rates
  //   const rows = [...data]
  //     .map(row => ({
  //       ...row,
  //       primary_type: String(row.primary_type || 'Unknown'),
  //       total_num: Number(row.total),
  //     }))
  //     .filter(row => Number.isFinite(row.total_num))
  //     .sort((a, b) => b.total_num - a.total_num)
  //     .slice(0, 2)
  // }

  if (table === 'great_recession_by_type') {
    const rows = [...data]
      .map(row => ({
        crime: String(row.primary_type || "Unknown"),
        total: Number(row.total)
      }))
      .filter(row => Number.isFinite(row.total))
      .sort((a, b) => b.total - a.total)
      .slice(0, 10)
  
    return (
      <div className="chart-block">
        <div className="chart-title">
          Most common crime types during the Great Recession (2007–2009)
        </div>
  
        <div className="chart-wrap">
          <ResponsiveContainer width="100%" height={320}>
            <BarChart
              layout="vertical"
              data={rows}
              margin={{ top: 10, right: 10, left: -75, bottom: 10 }}
            >
              <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
  
              <XAxis
                type="number"
                tick={{ fontSize: 11 }}
              />
  
              <YAxis
                type="category"
                dataKey="crime"
                width={220}
                tick={{ fontSize: 11 }}
              />
  
              <Tooltip />
  
              <Bar
                dataKey="total"
                fill="#3b82f6"
                radius={[0, 6, 6, 0]}
              />
            </BarChart>
          </ResponsiveContainer>
        </div>
      </div>
    )
  }

  if (table === 'monthly_crimes') {
  const monthNames = [
    'Jan','Feb','Mar','Apr','May','Jun',
    'Jul','Aug','Sep','Oct','Nov','Dec'
  ]

  const rows = [...data]
    .map(row => ({
      month_num: Number(row.month),
      total_crimes_num: Number(row.total_crimes),
      month_label: monthNames[Number(row.month) - 1]
    }))
    .filter(row => Number.isFinite(row.month_num) && Number.isFinite(row.total_crimes_num))
    .sort((a, b) => a.month_num - b.month_num)

  return (
    <div className="chart-block">
      <div className="chart-title">Crime trends by month</div>

      <div className="chart-wrap">
        <ResponsiveContainer width="100%" height={290}>
          <BarChart data={rows}>
            <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />

            <XAxis dataKey="month_label" />

            <YAxis />

            <Tooltip />

            <Bar
              dataKey="total_crimes_num"
              fill="#6366f1"
              radius={[6,6,0,0]}
            />

          </BarChart>
        </ResponsiveContainer>
      </div>
    </div>
  )
}

if (table === 'christmas_vs_nonchristmas_by_type') {
  const CHRISTMAS_DAYS = 31;
  const NON_CHRISTMAS_DAYS = 365 - 31;

  // Transform data into grouped chart format
  const crimeTypes = ["THEFT", "BURGLARY", "ROBBERY"];
  const groupedData = crimeTypes.map(type => {
    const christmasRow = data.find(
      row => row.primary_type === type && row.day_type === 'Christmas'
    );
    const nonChristmasRow = data.find(
      row => row.primary_type === type && row.day_type === 'Non-Christmas'
    );

    const christmasPerDay = christmasRow ? Number(christmasRow.total) / CHRISTMAS_DAYS : 0;
    const nonChristmasPerDay = nonChristmasRow ? Number(nonChristmasRow.total) / NON_CHRISTMAS_DAYS : 0;

    return {
      crime: type,
      Christmas: christmasPerDay,
      "Non-Christmas": nonChristmasPerDay
    };
  });

  return (
    <div className="chart-block">
      <div className="chart-title">Crime Comparison: Christmas vs Non-Christmas (Per Day)</div>
      <div className="chart-wrap">
        <ResponsiveContainer width="100%" height={320}>
          <BarChart
            data={groupedData}
            margin={{ top: 20, right: 40, left: 10, bottom: 40 }}
          >
            <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
            <XAxis dataKey="crime" angle={-35} textAnchor="end" height={80} />
            <YAxis tick={{ fontSize: 12 }} />
            <Tooltip
              formatter={(value, name) => `${value.toFixed(2)} per day`}
            />
            <Bar dataKey="Christmas" fill="#dc2626" radius={[6,6,0,0]} />
            <Bar dataKey="Non-Christmas" fill="#0d9488" radius={[6,6,0,0]} />
          </BarChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}

if (table === 'halloween_vs_nonhalloween_by_type') {
  const HALLOWEEN_DAYS = 1 * 20;       // 1 day per year × 20 years
  const NON_HALLOWEEN_DAYS = (365 - 1) * 20; // rest of days × 20 years

  // Focus on top 3 public disturbance / assault crimes
  const crimeTypes = ["ASSAULT", "BATTERY", "PUBLIC PEACE VIOLATION"];

  const groupedData = crimeTypes.map(type => {
    const halloweenRow = data.find(
      row => row.primary_type === type && row.day_type === 'Halloween'
    );
    const nonHalloweenRow = data.find(
      row => row.primary_type === type && row.day_type === 'Non-Halloween'
    );

    const halloweenPerDay = halloweenRow ? Number(halloweenRow.total) / HALLOWEEN_DAYS : 0;
    const nonHalloweenPerDay = nonHalloweenRow ? Number(nonHalloweenRow.total) / NON_HALLOWEEN_DAYS : 0;

    return {
      crime: type,
      Halloween: halloweenPerDay,
      "Non-Halloween": nonHalloweenPerDay
    };
  });

  return (
    <div className="chart-block">
      <div className="chart-title">
        Crime Comparison: Halloween vs Non-Halloween (Average per Day)
      </div>
      <div className="chart-wrap">
        <ResponsiveContainer width="100%" height={320}>
          <BarChart
            data={groupedData}
            margin={{ top: 20, right: 40, left: 10, bottom: 40 }}
          >
            <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
            <XAxis dataKey="crime" angle={-35} textAnchor="end" height={80} />
            <YAxis tick={{ fontSize: 12 }} />
            <Tooltip
              formatter={(value, name) => `${value.toFixed(2)} per day`}
            />
            <Bar dataKey="Halloween" fill="#a3a3a3" radius={[6, 6, 0, 0]} />
            <Bar dataKey="Non-Halloween" fill="#14b8a6" radius={[6, 6, 0, 0]} />
          </BarChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}
  
if (table === 'transit_vs_commercial_robbery_count') {
  const rows = [...data]
    .map(row => ({
      ...row,
      location_type: String(row.location_type || 'Unknown'),
      robbery_count_num: Number(row.robbery_count),
      total_crimes_num: Number(row.total_crimes),
      robbery_rate_num: Number(row.robbery_rate),
    }))
    .filter(row => Number.isFinite(row.robbery_rate_num))

  return (
    <div className="chart-block">
      <div className="chart-title">Robbery rate: Transit vs Commercial</div>
      <div className="chart-wrap">
        <ResponsiveContainer width="100%" height={290}>
          <BarChart data={rows}>
            <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
            <XAxis dataKey="location_type" />
            <YAxis />
            <Tooltip />
            <Bar dataKey="robbery_rate_num" fill="#ef4444" radius={[6, 6, 0, 0]} />
          </BarChart>
        </ResponsiveContainer>
      </div>
    </div>
  )
}


  return (
    <div className="chart-block">
      <div className="chart-title">Visualization</div>
      <p className="chart-empty">A chart for this question is coming soon.</p>
    </div>
  )




    return (
      <div className="chart-block">
        <div className="chart-title">Visualization</div>
        <p className="chart-empty">A chart for this question is coming soon.</p>
      </div>
    )

}

