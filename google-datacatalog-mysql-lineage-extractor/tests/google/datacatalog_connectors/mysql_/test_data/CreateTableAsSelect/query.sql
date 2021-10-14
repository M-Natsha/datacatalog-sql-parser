CREATE TABLE random_table AS
SELECT
  users.*,
  cars.id as car_id,
  cars.vendor,
  cars.model
FROM
  users
  LEFT JOIN cars ON users.id = cars.user_Id;