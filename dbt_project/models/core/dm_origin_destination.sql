with trips_data as (
    select * from {{ ref('fact_trips') }}
)

select
    -- Route Grouping (Tuyến đường)
    service_type,
    {{ dbt.date_trunc("month", "pickup_datetime") }} as report_month,

    -- Lưu ý: Bạn cần join với dim_zones 2 lần để lấy tên Zone nếu fact chỉ có ID
    -- Ở đây giả định fact_trips đã có tên hoặc ID zone
    pickup_zone,
    dropoff_zone,

    -- Metrics
    count(tripid) as trip_count,

    -- Xếp hạng độ phổ biến của tuyến đường
    rank() over (partition by service_type, {{ dbt.date_trunc("month", "pickup_datetime") }} order by count(tripid) desc) as route_popularity_rank,

    avg(trip_distance) as avg_distance,
    avg(total_amount) as avg_cost_per_trip,

    -- Tính năng suất tuyến đường (Tiền thu được trên mỗi dặm)
    sum(total_amount) / nullif(sum(trip_distance), 0) as revenue_per_mile

from trips_data
where pickup_zone is not null
  and dropoff_zone is not null
group by 1, 2, 3, 4