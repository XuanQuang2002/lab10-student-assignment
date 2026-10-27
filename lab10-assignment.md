Lab 10: Airflow
Trong bài lab này, bạn sẽ học cách sử dụng Apache Airflow để cấu hình, lên lịch và chạy các Pipeline thông qua các Operator cơ bản (BashOperator, PythonOperator, BranchPythonOperator, PostgresOperator, v.v.).
Bài lab gồm hai phần:

1. Thực hành trên lớp: Làm quen với Airflow.
2. Bài tập về nhà: Xây dựng một ETL Pipeline hoàn chỉnh.

Phần 1: Thực hành trên lớp (Tutorial)
Trong phần này, chúng ta sẽ thực hành xây dựng 2 luồng dữ liệu (Data Pipelines) thực tế với các Public API mở:
• first_dag (ETL Cơ bản - Tuyến tính): Mục tiêu: Xây dựng một luồng hoàn toàn bằng các BashOperator. Các tác vụ sẽ tuần tự: tải dữ liệu giá Bitcoin hiện tại từ API (api.coindesk.com) dưới dạng JSON, sử dụng lệnh grep để trích xuất dòng chứa giá USD, in kết quả ra log, và cuối cùng dọn dẹp không gian đĩa bằng cách xoá đi các file tạm.
• second_dag (ETL Nâng cao - Có rẽ nhánh và chạy song song): Mục tiêu: Kết hợp PythonOperator và PostgresOperator để làm Data Pipeline phức tạp. Luồng này sẽ gọi API randomuser.me để sinh ra một người dùng ngẫu nhiên. Sau đó, một BranchPythonOperator sẽ kiểm tra giới tính (gender) của người dùng đó. Nếu là nữ, rẽ vào nhánh process_female; nếu là nam, rẽ vào nhánh process_male. Cả hai nhánh sẽ định dạng tên và tạo câu lệnh SQL (INSERT) để đẩy vào các bảng tương ứng (female_users hoặc male_users) trong cơ sở dữ liệu PostgreSQL.
Trong thư mục dags/, bạn sẽ thấy các file \_stub.py (như first_dag_stub.py, second_dag_stub.py). Đây là các file mẫu (template) mô phỏng lại luồng chạy (DAG) này nhưng bị thiếu mất phần cài đặt quan hệ phụ thuộc (Dependencies).
Nhiệm vụ của bạn:

1. Mở các file \_stub.py này ra.
2. Cuộn xuống dưới cùng của file, tìm các vị trí được đánh dấu # TODO 1 và # TODO 2.
3. Điền mã nguồn tương ứng để thiết lập Dependencies giữa các task (sử dụng toán tử bitshift >>). Ví dụ: task_one >> task_two.
4. Tuỳ chọn: Thử thay thế các DummyOperator bằng các Operator thực tế (như BashOperator hoặc PythonOperator - có thể tham khảo từ thư mục lab10-solution) và xem chúng chạy trên Airflow UI.

Phần 2: Bài tập về nhà (D&D Character Generator)
Kristiine, một kỹ sư Java, thường xuyên chơi trò chơi nhập vai Dungeons & Dragons (D&D) với bạn bè vào mỗi tối Thứ Sáu. Cô ấy nhận thấy mỗi lần bắt đầu một cuộc phiêu lưu mới hoặc có ai đó trong game bị "chết", mọi người lại mất hàng giờ chỉ để tạo nhân vật mới. Để tiết kiệm thời gian, cô ấy muốn tự động hóa quy trình này.
Nhiệm vụ của bạn: Hãy viết một DAG Airflow có tên là dnd_weekly_characters trong file dags/dnd_dag.py để mỗi tuần định kỳ tự động tạo ra 5 nhân vật D&D mới, sau đó nạp chúng vào cơ sở dữ liệu postgres của Airflow.
Yêu cầu chi tiết về dữ liệu nhân vật:
Nhân vật phải là các tân binh cấp thấp (level 1 đến 3). Bảng dnd_characters trong Database cần có các cột VARCHAR sau:

1. name: Tên nhân vật. Bạn có thể sử dụng thư viện faker (đã được cài sẵn) để tạo tên ngẫu nhiên.
2. attributes: Một chuỗi biểu diễn 6 chỉ số (Strength, Dexterity, Constitution, Intelligence, Wisdom, Charisma). Mức điểm nằm trong khoảng 6-18 cho Strength và 2-18 cho các chỉ số còn lại. Ví dụ: "[16, 12, 10, 6, 10, 14]".
3. level: Cấp độ nhân vật (Random từ 1 đến 3).
4. race: Chủng tộc (Lấy random từ API).
5. class: Lớp nhân vật (Lấy random từ API).
6. languages: Ngôn ngữ nhân vật biết (Bất kỳ).
7. profficiency_choices: Kỹ năng thành thạo (Bất kỳ).
8. spells: Danh sách các phép thuật. Lưu ý:
   – Các phép thuật phải được lấy từ API.
   – Chỉ chọn các phép thuật tối đa là level 2.
   – Số lượng phép thuật tối đa được chọn = level + 3.
   – Lưu ý một số Class (như Fighter) không biết dùng phép, lúc này spells = none.
   API sử dụng: Sử dụng dữ liệu từ D&D 5e API. Đây là một Public API miễn phí, không yêu cầu xác thực. (Bạn không được phép chỉnh sửa Dockerfile hay cài thêm các package bên ngoài nào khác ngoài các thư viện gốc của Python và thư viện faker đã cung cấp).
   Yêu cầu về cấu trúc Pipeline (DAG):
   Để đảm bảo hiệu suất, các bước xử lý không nên dồn hết vào một hàm. Bạn bắt buộc phải tuân thủ nghiêm ngặt số lượng các Operator sau:
   • 2 Dummy Operators: Phải có một node start và một node end. Riêng node end bắt buộc phải có thuộc tính trigger_rule='none_failed'.
   • 4 Python Operators: Chia nhỏ logic thành các phần (Sinh base stats, gọi API, map dữ liệu, ghi log...).
   • 1 Branch Python Operator: Dùng để rẽ nhánh. Ví dụ: Nếu việc gọi API thành công thì tiếp tục làm dữ liệu, nếu API bị sập thì báo lỗi.
   • 1 Postgres Operator: Chịu trách nhiệm khởi tạo bảng và chèn dữ liệu. Khuyến khích sử dụng Jinja Templating kết hợp với XCom để kéo dữ liệu từ task Python và INSERT vào bảng.
   Hướng dẫn cài đặt và nộp bài
9. Khởi tạo môi trường Airflow: Chạy lệnh sau để thiết lập cơ sở dữ liệu và tạo tài khoản mặc định admin/admin.
   docker-compose up airflow-init
10. Bật các Services:
    docker-compose up -d
11. Truy cập Airflow: Mở http://localhost:8080, đăng nhập bằng admin/admin. Mở các DAG lên, unpause và Trigger để test code của bạn.
12. Kiểm tra dữ liệu: Để kiểm chứng code của bạn có INSERT dữ liệu thành công hay không, hãy kết nối vào PostgreSQL:
    docker-compose exec postgres psql -U airflow -d airflow -c "SELECT \* FROM dnd_characters;"
