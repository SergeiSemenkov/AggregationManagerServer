CREATE SCHEMA am AUTHORIZATION postgres;

-- am.aggregations definition

CREATE TABLE am.aggregations (
	id uuid NOT NULL DEFAULT gen_random_uuid(),
	aggregation_name varchar(255) NULL,
	table_name varchar(255) NULL,
	query text NULL,
	last_schema_update timestamptz NULL,
	scheduling_period varchar(255) NULL,
	scheduling_strategy varchar(255) NULL,
	process_group_id varchar(255) NULL,
	start_nifi_process_id varchar(255) NULL,
	is_generated_nifi_process bool NULL,
	created_by varchar(255) NULL,
	last_modified_by varchar(255) NULL,
	CONSTRAINT aggregations_pkey PRIMARY KEY (id)
);

-- am.events definition

CREATE TABLE am.events (
	id uuid NOT NULL DEFAULT gen_random_uuid(),
	aggregation_id uuid NULL,
	date_time timestamptz NULL,
	event_type varchar(255) NULL,
	event_message text NULL,
	CONSTRAINT events_pkey PRIMARY KEY (id)
);


-- am.settings definition


CREATE TABLE am.settings (
	default_template_id varchar(255) NULL
);

insert into am.settings select NULL;

-- am.users definition

CREATE TABLE am.users (
	id uuid NOT NULL DEFAULT gen_random_uuid(),
	user_name varchar(255) NULL,
	is_admin bool NULL,
	is_power_user bool NULL,
	CONSTRAINT users_pkey PRIMARY KEY (id)
);
