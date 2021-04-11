
-- FILE (relative to current file directory),DATE_UPDATE,START_DATE,END_DATE,SOUTHERN_MOST_LATITUDE,NORTHERN_MOST_LATITUDE,WESTERN_MOST_LONGITUDE,EASTERN_MOST_LONGITUDE,MINIMUM_DEPTH,MAXIMUM_DEPTH,UPDATE_INTERVAL,SIZE (in bytes),GDAC_CREATION_DATE,GDAC_UPDATE_DATE,DATA_MODE (R: real-time D: delayed mode M: mixed P: provisional),PARAMETERS (space delimited CF standard names)

CREATE SEQUENCE index_file_id_sequence
    INCREMENT 1
    START 100000
    MINVALUE 1
    MAXVALUE 9223372036854775807
    CACHE 1;


CREATE TABLE public.index_file
(
    file_id integer NOT NULL DEFAULT nextval('index_file_id_sequence'::regclass),
    file_name character varying NOT NULL,
    date_loaded timestamp with time zone,
    date_update timestamp with time zone,
    start_date timestamp with time zone,
    end_date timestamp with time zone,
    southern_most_latitude float,
    northern_most_latitude float,
    western_most_longitude float,
    eastern_most_longitude float,
    minimum_depth float,
    maximum_depth float,
    update_interval character varying,
    size int,
    gdac_creation_date timestamp with time zone,
    gdac_update_date timestamp with time zone,
    data_mode character,
    parameters character varying,

    CONSTRAINT index_file_pk PRIMARY KEY (file_id),
    CONSTRAINT index_file_unique_file UNIQUE (file_name)
)


