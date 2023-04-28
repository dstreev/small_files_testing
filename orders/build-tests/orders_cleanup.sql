!record results/bl_cleanup.out
SELECT 'START DROP DATABASE to ensure clean run', `current_timestamp`();
DROP DATABASE IF EXISTS ${DB} CASCADE;
SELECT 'END DROP DATABASE to ensure clean run', `current_timestamp`();
!record