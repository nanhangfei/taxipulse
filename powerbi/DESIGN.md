# Power BI Design Docs

## Datasets
1. **Batch (Import, daily refresh)** — ANALYTICS_DB.MARTS via BI_WH
   - Model: star — fct_trips ← dim_zones, dim_date
2. **Near-realtime (DirectQuery)** — RT_DB.PUBLIC.RT_ZONE_DEMAND_5MIN

## Key DAX measures
- Total Revenue = SUM(fct_trips[total_amount])
- Avg Fare = AVERAGE(fct_trips[fare_amount])
- Tip Rate = DIVIDE(SUM(fct_trips[tip_amount]), SUM(fct_trips[fare_amount]))
- Trips YoY % = VAR prev = CALCULATE([Trips], SAMEPERIODLASTYEAR(dim_date[date]))
                RETURN DIVIDE([Trips] - prev, prev)

## Pages
1. Executive: monthly revenue trend, YoY, service mix
2. Geography: zone heatmap (top pickup/dropoff), borough drilldown
3. Operations (DirectQuery): live 5-min zone demand, anomaly feed
## Mode rationale
Import for marts (fast, cheap on BI_WH, daily is enough);
DirectQuery only where freshness < 15 min is required.
