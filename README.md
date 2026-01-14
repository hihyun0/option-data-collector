### 저장된 데이터 확인 방법

live.db 혹은 다름파일이름.db를 Artifacts 섹션을 통해서 zip 파일로 다운받고
로컬에서 터미널을 통해 열어서 데이터가 저장되었는지 확인.

터미널에 아래 코드를 복붙하면 table과 row 갯수 결과가 보이면 성공.

sqlite3 live.db
.tables
SELECT COUNT(*) FROM option_data;
