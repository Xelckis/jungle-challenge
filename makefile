DB_URL=postgres://jungle_user:jungle_password@127.0.0.1:5432/jungle_db?sslmode=disable

migrate-up:
	docker run --rm -v $(PWD)/migrations:/migrations:z --network host migrate/migrate \
	-path=/migrations/ -database "$(DB_URL)" up

migrate-down:
	docker run --rm -v $(PWD)/migrations:/migrations:z --network host migrate/migrate \
	-path=/migrations/ -database "$(DB_URL)" down -all
