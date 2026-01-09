with trips_data as (
    select * from {{ ref('fact_trips') }}
)

select
    service_type,
    pickup_zone,
    payment_type_describe, -- Ví dụ: Credit Card, Cash, No Charge

    count(tripid) as total_trips,

    -- Chỉ tính % Tip trên các chuyến có trả tiền (Total > 0)
    count(case when tip_amount > 0 then 1 end) as count_trips_with_tip,

    -- Tỷ lệ số chuyến có Tip
    count(case when tip_amount > 0 then 1 end) * 100.0 / count(tripid) as percentage_trips_with_tip,

    -- Trung bình tiền Tip
    avg(tip_amount) as avg_tip_amount,

    -- % Tiền Tip so với giá cước (Chỉ tính Credit Card vì Cash thường không ghi nhận Tip chuẩn)
    avg(
        case
            when fare_amount > 0 and payment_type_describe = 'Credit Card'
            then (tip_amount / fare_amount) * 100
            else null
        end
    ) as avg_tip_percentage

from trips_data
where total_amount > 0
group by 1, 2, 3
-- Lọc bớt các nhóm quá nhỏ
having count(tripid) > 10