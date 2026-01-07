with trips_data as (
    select * from {{ ref('fact_trips') }}
)

select
    -- Dimensions
    service_type,
    {{ dbt.date_trunc("month", "pickup_datetime") }} as report_month,
    extract(dow from pickup_datetime) as day_of_week, -- 0=Sunday, 6=Saturday
    extract(hour from pickup_datetime) as hour_of_day,

    -- Metrics
    count(tripid) as total_trips,

    -- Tính thời gian chuyến đi (phút)
    avg(date_diff('minute', pickup_datetime, dropoff_datetime)) as avg_trip_duration_minutes,

    -- Tính khoảng cách trung bình
    avg(trip_distance) as avg_trip_distance_miles,

    -- Tốc độ trung bình (Miles per Hour - MPH)
    -- Logic: Distance / (Duration_in_minutes / 60). Nullif để tránh lỗi chia cho 0
    avg(
        case
            when date_diff('minute', pickup_datetime, dropoff_datetime) > 0
            then trip_distance / (date_diff('minute', pickup_datetime, dropoff_datetime) / 60.0)
            else null
        end
    ) as avg_speed_mph,

    -- Doanh thu trung bình mỗi phút (để xem hiệu suất kiếm tiền của tài xế)
    avg(
        case
            when date_diff('minute', pickup_datetime, dropoff_datetime) > 0
            then total_amount / (date_diff('minute', pickup_datetime, dropoff_datetime))
            else null
        end
    ) as avg_revenue_per_minute

from trips_data
-- Lọc bỏ các chuyến đi lỗi (khoảng cách = 0 hoặc thời gian = 0)
where trip_distance > 0
  and dropoff_datetime > pickup_datetime
group by 1, 2, 3, 4
order by 1, 2, 3, 4