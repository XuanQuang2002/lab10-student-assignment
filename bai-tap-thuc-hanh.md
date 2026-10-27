## 11. Thực hành: Xây dựng DAG

### Bài 1 - First DAG: ETL tuyến tính

Mục tiêu: xây dựng pipeline ETL chạy tuần tự bằng `BashOperator`.

Luồng xử lý:

1. **Extract:** Dùng `curl` tải dữ liệu giá Bitcoin từ API Coindesk.
2. **Transform:** Dùng `grep` trích xuất giá USD từ JSON.
3. **Log:** Dùng `cat` in kết quả ra log.
4. **Cleanup:** Dùng `rm` xóa các file tạm.

Dependencies:

```
task_one >> task_two >> task_three >> task_four
```

Bài tập giúp thực hành cách khai báo task và nối một pipeline tuyến tính.

### Bài 2 - Second DAG: ETL có rẽ nhánh

Mục tiêu: xây dựng pipeline có điều kiện bằng `PythonOperator`, `BranchPythonOperator` và `PostgresOperator`.

Luồng xử lý:

1. Gọi API `randomuser.me` và lưu dữ liệu người dùng.
2. Đọc giới tính từ JSON.
3. Chọn nhánh `process_female` hoặc `process_male`.
4. Tạo câu lệnh SQL INSERT tương ứng và nạp dữ liệu vào PostgreSQL.
5. Hội tụ các nhánh tại `join_tasks` rồi kết thúc.

Luồng tổng quát:

```
Get Random User
       ↓
  Check Gender
    ↙       ↘
Female      Male
  ↓           ↓
Insert DB   Insert DB
    ↘       ↙
      Join
       ↓
      End
```

Bài tập giúp hiểu cách tạo nhánh và tổ chức workflow phức tạp hơn.
