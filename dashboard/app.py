import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
import os

# 1. Cấu hình trang
st.set_page_config(page_title="NYC Taxi Dashboard", layout="wide", page_icon="🚖")
st.title("🚖 NYC Taxi Analytics Dashboard")

# 2. Định nghĩa đường dẫn
DB_PATH = '/data/nyc_taxi_view.duckdb'


# 3. Kết nối DB
@st.cache_resource
def get_connection():
    con = duckdb.connect(DB_PATH, read_only=True)
    return con


# 4. Kiểm tra file tồn tại
if not os.path.exists(DB_PATH):
    st.warning("⚠️ Đang chờ dữ liệu...")
    st.info(f"Streamlit đang tìm file tại: `{DB_PATH}`")
    st.markdown("Hãy chạy pipeline Airflow để tạo file này.")
    if st.button("🔄 Tải lại trang"):
        st.rerun()
    st.stop()

# 5. Xử lý chính
try:
    con = get_connection()

    # Kiểm tra bảng quan trọng nhất tồn tại chưa
    tables = con.sql("SHOW TABLES").df()
    if 'dm_monthly_zone' not in tables['name'].values:
        st.error("❌ Kết nối thành công nhưng chưa thấy các bảng Data Mart (dm_...).")
        st.info("Danh sách bảng hiện có: " + ", ".join(tables['name'].tolist()))
        st.stop()

    # --- TẠO TABS ---
    tab1, tab2, tab3 = st.tabs(["📊 Tổng quan Doanh thu", "⏱️ Hiệu suất Vận hành", "🗺️ Tuyến đường & Tip"])

    # ==================================================
    # TAB 1: TỔNG QUAN (Dùng dm_monthly_zone)
    # ==================================================
    with tab1:
        st.subheader("Doanh thu & Tăng trưởng")

        # Load data
        df_monthly = con.sql("SELECT * FROM dm_monthly_zone").df()

        if not df_monthly.empty:
            # KPI Cards
            total_rev = df_monthly['revenue_monthly_total_amount'].sum()
            total_trips = df_monthly['total_monthly_trips'].sum()
            avg_dist = df_monthly['avg_monthly_trip_distance'].mean()

            col1, col2, col3 = st.columns(3)
            col1.metric("💰 Tổng Doanh Thu", f"${total_rev:,.0f}")
            col2.metric("🚖 Tổng Chuyến Đi", f"{total_trips:,.0f}")
            col3.metric("📏 Quãng đường TB", f"{avg_dist:.2f} miles")

            st.divider()

            # Biểu đồ Doanh thu theo tháng
            col_chart1, col_chart2 = st.columns(2)

            with col_chart1:
                st.markdown("**Xu hướng Doanh thu theo Tháng**")
                # Group by month để vẽ line chart
                df_trend = df_monthly.groupby(['revenue_month', 'service_type'])[
                    'revenue_monthly_total_amount'].sum().reset_index()
                fig_trend = px.line(df_trend, x='revenue_month', y='revenue_monthly_total_amount', color='service_type',
                                    markers=True)
                st.plotly_chart(fig_trend, use_container_width=True)

            with col_chart2:
                st.markdown("**Top 10 Khu vực Doanh thu cao nhất**")
                # Group by Zone
                df_zone = df_monthly.groupby('revenue_zone')[
                    'revenue_monthly_total_amount'].sum().reset_index().sort_values(by='revenue_monthly_total_amount',
                                                                                    ascending=False).head(10)
                fig_zone = px.bar(df_zone, x='revenue_monthly_total_amount', y='revenue_zone', orientation='h',
                                  text_auto='.2s')
                fig_zone.update_layout(yaxis={'categoryorder': 'total ascending'})
                st.plotly_chart(fig_zone, use_container_width=True)

    # ==================================================
    # TAB 2: VẬN HÀNH (Dùng dm_hourly_operation)
    # ==================================================
    with tab2:
        st.subheader("Phân tích Giờ cao điểm & Tốc độ")

        # Load data (Cần kiểm tra bảng này có chưa)
        if 'dm_hourly_operation' in tables['name'].values:
            df_ops = con.sql("SELECT * FROM dm_hourly_operation").df()

            # Filter
            service_filter = st.selectbox("Chọn loại xe:", df_ops['service_type'].unique())
            df_ops_filtered = df_ops[df_ops['service_type'] == service_filter]

            col_ops1, col_ops2 = st.columns(2)

            with col_ops1:
                st.markdown("**Heatmap: Mật độ chuyến đi (Thứ vs Giờ)**")
                # Pivot data cho heatmap
                heatmap_data = df_ops_filtered.groupby(['day_of_week', 'hour_of_day'])[
                    'total_trips'].sum().reset_index()
                fig_heat = px.density_heatmap(heatmap_data, x='hour_of_day', y='day_of_week', z='total_trips',
                                              nbinsx=24, nbinsy=7, color_continuous_scale='Viridis')
                st.plotly_chart(fig_heat, use_container_width=True)

            with col_ops2:
                st.markdown("**Tốc độ trung bình theo giờ trong ngày**")
                speed_data = df_ops_filtered.groupby('hour_of_day')['avg_speed_mph'].mean().reset_index()
                fig_speed = px.line(speed_data, x='hour_of_day', y='avg_speed_mph', markers=True, title="Tốc độ (MPH)")
                st.plotly_chart(fig_speed, use_container_width=True)
        else:
            st.warning("Chưa tìm thấy bảng `dm_hourly_operation`.")

    # ==================================================
    # TAB 3: TUYẾN ĐƯỜNG & TIP (Dùng dm_origin_destination & dm_tipping)
    # ==================================================
    with tab3:
        col_route, col_tip = st.columns([1, 1])

        with col_route:
            st.subheader("🗺️ Top Tuyến đường phổ biến")
            if 'dm_origin_destination' in tables['name'].values:
                # Lấy Top 10 tuyến đường đông nhất
                query_route = """
                              SELECT pickup_zone, \
                                     dropoff_zone, \
                                     sum(trip_count)        as total_trips, \
                                     avg(avg_cost_per_trip) as avg_cost
                              FROM dm_origin_destination
                              GROUP BY 1, 2
                              ORDER BY total_trips DESC LIMIT 10 \
                              """
                df_route = con.sql(query_route).df()
                st.dataframe(df_route, use_container_width=True)
            else:
                st.warning("Chưa tìm thấy bảng `dm_origin_destination`.")

        with col_tip:
            st.subheader("💸 Hành vi Tip & Thanh toán")
            if 'dm_tipping' in tables['name'].values:
                # Phân tích Tip theo hình thức thanh toán
                query_tip = """
                            SELECT payment_type_describe, avg(avg_tip_percentage) as tip_pct, sum(total_trips) as trips
                            FROM dm_tipping
                            WHERE payment_type_describe IS NOT NULL
                            GROUP BY 1
                            ORDER BY trips DESC \
                            """
                df_tip = con.sql(query_tip).df()

                fig_tip = px.bar(df_tip, x='payment_type_describe', y='tip_pct',
                                 title="% Tip trung bình theo loại thanh toán", text_auto='.1f')
                st.plotly_chart(fig_tip, use_container_width=True)
            else:
                st.warning("Chưa tìm thấy bảng `dm_tipping`.")

except Exception as e:
    st.error(f"Đã xảy ra lỗi: {e}")
    st.cache_resource.clear()