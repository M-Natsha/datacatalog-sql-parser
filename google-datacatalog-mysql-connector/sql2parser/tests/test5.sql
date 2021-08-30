CREATE TABLE random_table(blabla varchar(25), balssf, bsdbs) AS
SELECT
  users.*,
  cars.id as car_id,
  cars.vendor,
  cars.model
FROM
  users
  LEFT JOIN cars ON users.id = cars.user_Id;