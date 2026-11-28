# T07 — Streamlit: Framework Dựng Dashboard Dữ liệu Nhanh

## 1. Câu hỏi gốc
Làm sao xây dựng một giao diện Dashboard nhiều panel có khả năng tự làm tươi định kỳ mỗi 10 giây, an toàn khi nhiều phiên trình duyệt truy cập đồng thời và không tạo lại kết nối DuckDB hay quét lại MinIO ở mỗi lần người dùng bấm nút?

## 2. Trực giác
Streamlit vận hành theo mô hình **chạy lại toàn bộ script Python từ trên xuống dưới** mỗi khi có tương tác. Nếu không có bộ nhớ đệm, mỗi lần nhấn filter là một lần mở kết nối DuckDB mới và tải extension lại từ đầu. Hai decorator `@st.cache_resource` (giữ tài nguyên dùng chung như root connection) và `@st.cache_data` (giữ bảng kết quả DataFrame theo TTL) kết hợp `@st.fragment(run_every="10s")` (chỉ làm tươi đúng 1 panel) giải quyết triệt để nút thắt này.

## 3. Cơ chế

```
[Phiên trình duyệt 1 (Thread A)] ──┐
[Phiên trình duyệt 2 (Thread B)] ──┼──► @st.cache_resource: get_root_connection() (1 root :memory:)
                                   │          │
                                   │          ▼  run_query() tạo cursor() riêng mỗi query
                                   └──► @st.cache_data(ttl=5): chia sẻ DataFrame kết quả 5s
                                              │
                                              ▼
                                        @st.fragment(run_every="10s") cập nhật từng panel
```

- **`@st.cache_resource`:** Chia sẻ **cùng một đối tượng** giữa mọi phiên/thread (dùng cho `get_root_connection()`). Vì dùng chung đa luồng, tuyệt đối không dùng chung con trỏ đột biến mà phải gọi `conn.cursor()` ở mỗi query.
- **`@st.cache_data(ttl=5)`:** Lưu bản sao kết quả (DataFrame) theo tham số truy vấn trong 5 giây để N người xem không nhân N lần truy vấn S3.
- **`@st.fragment(run_every="10s")`:** Cô lập chu kỳ rerun vào đúng một panel thay vì nháy toàn bộ trang.

## 4. Ví dụ số
- Khởi tạo root connection (`LOAD httpfs`, `LOAD delta`, `CREATE SECRET`) mất khoảng vài trăm ms; nhờ `@st.cache_resource`, thao tác này chỉ chạy **1 lần duy nhất** khi server Streamlit khởi động.
- Khi 5 tab mở song song và chạy `25` truy vấn đồng thời qua `run_query()`, tất cả đều dùng `cursor()` riêng trên `:memory:`, **0 lỗi khóa**, `0` race condition (`tests/unit/test_dashboard_db.py`).

## 5. Điểm mạnh / Điểm yếu / Khi nào không dùng
- **Điểm mạnh:** Dựng ứng dụng dữ liệu bằng Python cực nhanh; tích hợp thẳng pandas/DuckDB; hỗ trợ fragment tự làm tươi cục bộ.
- **Điểm yếu:** Mô hình rerun dễ gây truy vấn thừa nếu đặt code ngoài cache/fragment; không phù hợp phân quyền doanh nghiệp phức tạp hoặc lập lịch gửi alert nền (alerting phải tách ra worker riêng ở Task 5.8).
- **Khi nào không dùng:** Hệ thống BI nhiều trăm người dùng cần RBAC chi tiết, dashboard đa tenant phức tạp (dùng Superset/Metabase/Grafana).

## 6. Hiểu lầm phổ biến
1. *Hiểu lầm:* Có thể gửi Telegram/Discord alert ngay bên trong `@st.fragment(run_every="10s")`.  
   *Sự thật:* Như vậy alert chỉ chạy khi có người mở trình duyệt và sẽ bắn lặp lại mỗi 10 giây gây bão cảnh báo. Alert phải là một process độc lập có lưu trạng thái (`Task 5.8`).
2. *Hiểu lầm:* Đóng gói Docker chỉ cần `pip install streamlit duckdb` là chạy offline được ngay.  
   *Sự thật:* DuckDB sẽ tìm tải extension `httpfs`/`delta` qua mạng lúc runtime nếu không cài sẵn vào `DUCKDB_EXTENSION_DIRECTORY` lúc build image (`src/dashboard/db.py:99-102`).

## 7. Trong repo này

| File:dòng | Symbol | Đoạn code | Giải thích dòng nào quan trọng |
| :--- | :--- | :--- | :--- |
| `src/dashboard/db.py:141-144` | `get_root_connection` | `@st.cache_resource(show_spinner=False)` | Cache duy nhất một root `:memory:` connection cho mọi thread Streamlit. |
| `src/dashboard/db.py:91-103` | `create_root_connection` | `DUCKDB_EXTENSION_DIRECTORY` | Cho phép trỏ thư mục extension cài sẵn trong container ở Task 5.9. |
| `src/dashboard/db.py:44-56` | `DatabaseConnectionError` | Lỗi kết nối có cấu trúc | Giúp UI hiển thị trạng thái *"Mất kết nối dữ liệu"* thân thiện thay vì crash traceback. |
| `src/dashboard/panels/common.py:177-195` | `get_repository`, `fetch_pacing` | `@st.cache_resource` + `@st.cache_data(ttl=5)` | Dùng chung `DashboardRepository` và cache bảng kết quả 5s theo chuỗi ISO `sim_now`/`event_date`. |
| `src/dashboard/app.py:34-52` | `_pacing_fragment` .. `_health_fragment` | `@st.fragment(run_every="10s")` | Tự làm tươi độc lập từng panel mỗi 10 giây mà không rerun toàn bộ trang. |

```python
# Trích nguyên văn từ src/dashboard/db.py:141-144
@st.cache_resource(show_spinner=False)
def get_root_connection() -> duckdb.DuckDBPyConnection:
    """Return process-wide cached DuckDB root connection for Streamlit threads."""
    return create_root_connection()
```

```python
# Trích nguyên văn từ src/dashboard/panels/common.py:177-195
@st.cache_resource(show_spinner=False)
def get_repository() -> DashboardRepository:
    """Return process-wide cached DashboardRepository bound to the cached DuckDB root."""
    return DashboardRepository(root_conn=get_root_connection())


@st.cache_data(ttl=5, show_spinner=False)
def fetch_sim_now() -> tuple[dict[str, Any], float]:
    t0 = time.perf_counter()
    data = get_repository().get_sim_now()
    return data, (time.perf_counter() - t0) * 1000.0


@st.cache_data(ttl=5, show_spinner=False)
def fetch_pacing(sim_now_iso: str, event_date_iso: str) -> tuple[pd.DataFrame, float]:
    t0 = time.perf_counter()
    sim_now = datetime.fromisoformat(sim_now_iso)
    event_date = date.fromisoformat(event_date_iso)
    df = get_repository().get_pacing(sim_now=sim_now, event_date=event_date)
```

```python
# Trích nguyên văn từ src/dashboard/app.py:34-36
@st.fragment(run_every="10s")
def _pacing_fragment(repo: DashboardRepository) -> None:
    render_pacing_panel(repo)
```

## 8. Tự kiểm tra
1. *Câu hỏi:* Phân biệt `@st.cache_resource` và `@st.cache_data` trong Dashboard của dự án?  
   *Đáp án:* `@st.cache_resource` dùng cho đối tượng tài nguyên dùng chung như `get_root_connection()`; `@st.cache_data(ttl=5)` dùng cho dữ liệu kết quả (`pd.DataFrame`) theo tham số truy vấn.
2. *Câu hỏi:* Vì sao `create_root_connection()` hỗ trợ biến `DUCKDB_EXTENSION_DIRECTORY`?  
   *Đáp án:* Để khi đóng gói container (Task 5.9), image cài sẵn extension `httpfs` và `delta` vào thư mục này, giúp container khởi động lại không cần Internet.
3. *Câu hỏi:* Điều gì xảy ra nếu đặt hàm truy vấn S3 ở ngoài `@st.fragment` mà dùng `st.rerun()` toàn trang?  
   *Đáp án:* Toàn bộ trang và cả 4 panel sẽ chạy lại và nháy giao diện mỗi lần làm tươi thay vì chỉ cập nhật từng panel độc lập.

## 9. Đọc thêm
- Streamlit Caching (`st.cache_resource` & `st.cache_data`): https://docs.streamlit.io/develop/concepts/architecture/caching
- Streamlit Fragments (`@st.fragment`): https://docs.streamlit.io/develop/api-reference/execution-flow/st.fragment
