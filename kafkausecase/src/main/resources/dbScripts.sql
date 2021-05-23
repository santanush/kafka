CREATE TABLE carstate."CAR_MOVEMENT_COUNT" (
	id serial NOT NULL,
	movement_date date NOT NULL,
	created_on timestamp NULL,
	car_count int8 NOT NULL,
	car_type varchar(100) NULL,
	created_by varchar(10) NULL,
	CONSTRAINT "CarMovementCount_pkey1" PRIMARY KEY (id)
);