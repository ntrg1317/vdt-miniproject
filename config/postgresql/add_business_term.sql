-- Cập nhật business_term cho từng bảng
UPDATE catalog.table_origin
SET business_term = 'Lịch sử báo cáo KTXH theo chỉ tiêu từng tháng cấp huyện'
WHERE tablename = 'history_bao_cao_ktxh_huyen_lac_duong_chi_tieu_thang_7753';

UPDATE catalog.table_origin
SET business_term = 'Loại hình tổ chức (xã/phòng/ban...)'
WHERE tablename = 'sys_organization_type';

UPDATE catalog.table_origin
SET business_term = 'Nhật ký xử lý báo cáo'
WHERE tablename = 'rp_process_log';

UPDATE catalog.table_origin
SET business_term = 'Báo cáo KTXH cấp huyện theo chỉ tiêu từng tháng'
WHERE tablename = 'bao_cao_ktxh_huyen_lac_duong_chi_tieu_thang_7753';

UPDATE catalog.table_origin
SET business_term = 'Chương trình/đề án liên quan đến báo cáo'
WHERE tablename = 'rp_program';

UPDATE catalog.table_origin
SET business_term = 'Danh sách hộ nghèo huyện Lạc Dương'
WHERE tablename = 'danhsach_hongheo_lacduong';

UPDATE catalog.table_origin
SET business_term = 'Cột động cho báo cáo'
WHERE tablename = 'rp_column';

UPDATE catalog.table_origin
SET business_term = 'Báo cáo KTXH cấp huyện từng tháng (mã 4702)'
WHERE tablename = 'bao_cao_thang_ktxh_huyen_lac_duong_4702';

UPDATE catalog.table_origin
SET business_term = 'Lịch sử báo cáo tháng KTXH cấp huyện (4702)'
WHERE tablename = 'history_bao_cao_thang_ktxh_huyen_lac_duong_4702';

UPDATE catalog.table_origin
SET business_term = 'Danh mục tổ chức hành chính (xã/phòng/ban)'
WHERE tablename = 'sys_organization';

UPDATE catalog.table_origin
SET business_term = 'Danh mục báo cáo'
WHERE tablename = 'rp_report';

UPDATE catalog.table_origin
SET business_term = 'Danh mục kỳ báo cáo'
WHERE tablename = 'rp_period';

UPDATE catalog.table_origin
SET business_term = 'Bảng dữ liệu dùng thử'
WHERE tablename = 'bang_test_demo';

UPDATE catalog.table_origin
SET business_term = 'Tỷ lệ hộ nghèo huyện Lạc Dương'
WHERE tablename = 'ty_le_ho_ngheo_lacduong';

UPDATE catalog.table_origin
SET business_term = 'Cột báo cáo tạm thời'
WHERE tablename = 'temp_rp_column';

UPDATE catalog.table_origin
SET business_term = 'Cấu hình quyền nhập liệu báo cáo'
WHERE tablename = 'rp_input_grant';

UPDATE catalog.table_origin
SET business_term = 'Bảng test dữ liệu lỗi'
WHERE tablename = 'bang_hong_test';

UPDATE catalog.table_origin
SET business_term = 'Dữ liệu thử nghiệm dùng để kéo từ nguồn khác'
WHERE tablename = 'demo_pull_data';

UPDATE catalog.table_origin
SET business_term = 'Lịch sử báo cáo KTXH theo phòng ban (7759)'
WHERE tablename = 'history_bc_ktxh_huyen_lac_duong_chi_tieu_thang_phong_ban_7759';

UPDATE catalog.table_origin
SET business_term = 'Báo cáo KTXH theo chỉ tiêu từng tháng của phòng ban'
WHERE tablename = 'bao_cao_ktxh_huyen_lac_duong_chi_tieu_thang_phong_ban_7759';

UPDATE catalog.table_origin
SET business_term = 'Dòng báo cáo tạm thời'
WHERE tablename = 'temp_rp_row';

UPDATE catalog.table_origin
SET business_term = 'Dòng báo cáo động'
WHERE tablename = 'rp_row';


UPDATE catalog.field_origin
SET business_term = 'Tổ chức phát hành báo cáo'
WHERE field = 'release_org';

UPDATE catalog.field_origin
SET business_term = 'Trạng thái của báo cáo (1: hoạt động, 0: ngừng)'
WHERE field = 'status';

UPDATE catalog.field_origin
SET business_term = 'Thời gian cập nhật gần nhất'
WHERE field = 'update_time';

UPDATE catalog.field_origin
SET business_term = 'Chuyển sang trạng thái'
WHERE field = 'to_state';

UPDATE catalog.field_origin
SET business_term = 'Đường dẫn index phân cấp'
WHERE field = 'idxpath';

UPDATE catalog.field_origin
SET business_term = 'Đơn vị chỉ tiêu'
WHERE field = 'ind_unit';

UPDATE catalog.field_origin
SET business_term = 'Loại cột dữ liệu'
WHERE field = 'column_type';

UPDATE catalog.field_origin
SET business_term = 'Giá trị thực tế tháng hiện tại (tt4)'
WHERE field = 'tt4';

UPDATE catalog.field_origin
SET business_term = 'Mã báo cáo định danh'
WHERE field = 'report_code';
