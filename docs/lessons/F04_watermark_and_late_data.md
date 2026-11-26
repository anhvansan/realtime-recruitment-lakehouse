# F04 — Watermark và Dữ liệu Đến Trễ (Late Data)

## 1. Câu hỏi gốc
Khi dữ liệu click đi đường vòng qua mạng yếu nên giữ nguyên `event_time` gốc nhưng đến chậm 5–15 phút, hệ thống phải đợi bao lâu trước khi "chốt" trạng thái khử trùng để không ngốn hết RAM?

## 2. Trực giác
**Watermark** giống như giờ đóng cổng lên tàu: trưởng ga tuyên bố *"Tôi tin rằng mọi hành khách có vé trước 10:20 đã tới ga rồi"* (`watermark = max_event_time - 10 minutes`). Sau mốc này, hệ thống dọn sạch danh sách kiểm vé cũ khỏi bộ nhớ (state store); người có vé 10:15 đến muộn sẽ bị lỡ chuyến ở tầng Silver (nhưng vẫn được lưu giữ an toàn ở phòng chờ Bronze!).

## 3. Cơ chế Bơm Dữ liệu Trễ và Ảnh hưởng Watermark

```
Sim Clock hiện tại = 10:30 ──► Watermark (delay=10m) = 10:20

  1. Event trễ 7 phút  (event_time = 10:23 >= 10:20) ➔ ✅ Chấp nhận vào Silver
  2. Event trễ 12 phút (event_time = 10:18 <  10:20) ➔ ❌ Bị loại ở Silver state dedup
                                                        └── Vẫn tồn tại ở Bronze & late_ids.jsonl!
```

1. **Tách bạch `event_time` và thời điểm gửi (`emit_sim_time`):** Trong `FaultInjector`, 2% sự kiện giữ nguyên `event_time` gốc nhưng bị trì hoãn bằng đồng hồ mô phỏng `5–15 phút` (`delay_min ~ U(5, 15)`).
2. **Vì sao phải tính trễ theo đồng hồ mô phỏng?** Ở `speed_factor=600`, 10 phút mô phỏng chỉ tương đương `1 giây` ngoài đời thật! Nếu tính nhầm 10 phút wall-clock thì hệ thống sẽ treo chờ hàng trăm ngàn phút mô phỏng.
3. **Phân loại ngưỡng `> 10 phút` trong Manifest:** Producer ghi rõ danh sách các `event_id` trễ vào `data/manifests/replay_<run_id>.late_ids.jsonl` để bài đối soát ở Task 3.5 kiểm tra xem những bản ghi vắng mặt ở Silver có khớp chính xác với nhóm trễ vượt watermark hay không.

## 4. Ví dụ số
Khi bơm 100.000 dòng với tỷ lệ `late_rate=0.02` (độ trễ phân bố đều `5..15` phút) và watermark đặt ở `10 phút`:
- Khoảng **~2.000** sự kiện bị trì hoãn.
- Một nửa số đó (`delay_minutes` từ `10..15` phút ➔ **~1.000** sự kiện) bị trễ vượt mốc watermark 10 phút.

## 5. Điểm mạnh / Điểm yếu / Khi nào không dùng
- **Điểm mạnh:** Giới hạn kích thước RAM của toán tử `dropDuplicatesWithinWatermark` ở tầng Silver; giúp kiểm chứng chính xác giữa Bronze (đủ 100%) và Silver.
- **Điểm yếu:** Mất một phần sự kiện quá trễ ở Silver nếu chọn watermark quá ngắn (đánh đổi bằng bộ nhớ nhỏ, sẽ thực nghiệm đo đạc chi tiết ở Task 3.7 ADR-006).
- **Khi nào không dùng:** Pipeline chỉ ghi append-only không cần state (như tầng Bronze) ➔ không dùng watermark để giữ 100% lịch sử.

## 6. Hiểu lầm phổ biến
1. *Hiểu lầm:* Watermark tự động sắp xếp lại thứ tự các event đang trễ.  
   *Sự thật:* Watermark chỉ là một ngưỡng thời gian đơn điệu tăng để **dọn state** và **loại event quá hạn**, không phải bộ đệm sắp xếp (sort buffer).

## 7. Trong repo này

| File:dòng | Symbol | Giải thích |
| :--- | :--- | :--- |
| `src/producer/fault_injector.py:168-227` | `FaultInjector.process_event` | Giữ nguyên `event_time`, đẩy bản ghi trễ vào `heapq` chậm `5..15` phút mô phỏng và ghi `LateRecord`. |
| `src/producer/replay_producer.py:725-762` | `delayed_heap` | Hàng đợi ưu tiên phát hành các sự kiện trễ đúng thời điểm đồng hồ mô phỏng bước tới. |
| `tests/unit/test_fault_injector.py:55-84` | `test_fault_rates_converge_on_100k_events` | Kiểm chứng tỷ lệ 2% trễ và xấp xỉ 50% vượt ngưỡng 10 phút trên 100.000 dòng. |

## 8. Tự kiểm tra
1. *Câu hỏi:* Vì sao trong `late_ids.jsonl` ta cần lưu cả `delay_minutes` lẫn `spend`?  
   *Đáp án:* Để ở Task 3.5, công cụ `reconcile.py` chứng minh tổng `SUM(spend)` của Silver cộng với `spend` của các event bị loại do trễ vượt watermark khớp tuyệt đối với `expected_spend_total` trong manifest.
2. *Câu hỏi:* Điều gì xảy ra với một bản ghi `late` có `delay_minutes = 12.5` tại tầng Bronze?  
   *Đáp án:* Bronze là bảng append-only không dùng watermark nên vẫn nhận và lưu trữ bình thường 100%.
