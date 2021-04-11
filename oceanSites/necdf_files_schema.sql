--
-- PostgreSQL database dump
--

-- Dumped from database version 12.1
-- Dumped by pg_dump version 12.1

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: coordinates; Type: TABLE; Schema: public; Owner: ubuntu
--

CREATE TABLE public.coordinates (
    file_id integer NOT NULL,
    name character varying NOT NULL,
    axis character(1),
    value double precision[]
);


ALTER TABLE public.coordinates OWNER TO ubuntu;

--
-- Name: file_id_sequence; Type: SEQUENCE; Schema: public; Owner: ubuntu
--

CREATE SEQUENCE public.file_id_sequence
    START WITH 100000
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.file_id_sequence OWNER TO ubuntu;

--
-- Name: file; Type: TABLE; Schema: public; Owner: ubuntu
--

CREATE TABLE public.file (
    file_id integer DEFAULT nextval('public.file_id_sequence'::regclass) NOT NULL,
    url character varying,
    file_name character varying NOT NULL,
    date_loaded timestamp with time zone,
    date_coords timestamp with time zone,
    site_code character varying,
    platform_code character varying,
    deployment_code character varying,
    geospatial_lat_min character varying,
    geospatial_lat_max character varying,
    geospatial_lon_min character varying,
    geospatial_lon_max character varying,
    geospatial_vertical_min character varying,
    geospatial_vertical_max character varying,
    title character varying,
    date_created timestamp with time zone,
    time_coverage_start timestamp with time zone,
    time_coverage_end timestamp with time zone,
    principal_investigator character varying
);


ALTER TABLE public.file OWNER TO ubuntu;

--
-- Name: file_coords; Type: VIEW; Schema: public; Owner: postgres
--

CREATE VIEW public.file_coords AS
 SELECT file.file_id,
    file.file_name,
    x.value[1] AS x,
    y.value[1] AS y,
    z.value[1] AS z
   FROM (((public.file
     JOIN public.coordinates x ON (((x.file_id = file.file_id) AND (lower((x.axis)::text) = 'x'::text))))
     JOIN public.coordinates y ON (((y.file_id = file.file_id) AND (lower((y.axis)::text) = 'y'::text))))
     JOIN public.coordinates z ON (((z.file_id = file.file_id) AND (lower((z.axis)::text) = 'z'::text))));


ALTER TABLE public.file_coords OWNER TO postgres;

--
-- Name: global_attributes; Type: TABLE; Schema: public; Owner: ubuntu
--

CREATE TABLE public.global_attributes (
    file_id integer NOT NULL,
    name character varying NOT NULL,
    type character varying,
    value character varying
);


ALTER TABLE public.global_attributes OWNER TO ubuntu;

--
-- Name: index_file_id_sequence; Type: SEQUENCE; Schema: public; Owner: pete
--

CREATE SEQUENCE public.index_file_id_sequence
    START WITH 100000
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.index_file_id_sequence OWNER TO pete;

--
-- Name: index_file; Type: TABLE; Schema: public; Owner: pete
--

CREATE TABLE public.index_file (
    file_id integer DEFAULT nextval('public.index_file_id_sequence'::regclass) NOT NULL,
    file_name character varying NOT NULL,
    date_loaded timestamp with time zone,
    date_update timestamp with time zone,
    start_date timestamp with time zone,
    end_date timestamp with time zone,
    southern_most_latitude double precision,
    northern_most_latitude double precision,
    western_most_longitude double precision,
    eastern_most_longitude double precision,
    minimum_depth double precision,
    maximum_depth double precision,
    update_interval character varying,
    size integer,
    gdac_creation_date timestamp with time zone,
    gdac_update_date timestamp with time zone,
    data_mode character(1),
    parameters character varying
);


ALTER TABLE public.index_file OWNER TO pete;

--
-- Name: variables; Type: TABLE; Schema: public; Owner: ubuntu
--

CREATE TABLE public.variables (
    file_id integer NOT NULL,
    variable character varying NOT NULL,
    name character varying,
    units character varying,
    dimensions character varying,
    aux_vars character varying,
    is_aux boolean,
    type character varying,
    is_coord boolean
);


ALTER TABLE public.variables OWNER TO ubuntu;

--
-- Name: variables_attributes; Type: TABLE; Schema: public; Owner: ubuntu
--

CREATE TABLE public.variables_attributes (
    file_id integer NOT NULL,
    variable character varying NOT NULL,
    name character varying NOT NULL,
    type character varying,
    value character varying
);


ALTER TABLE public.variables_attributes OWNER TO ubuntu;

--
-- Name: variable_names; Type: VIEW; Schema: public; Owner: postgres
--

CREATE VIEW public.variable_names AS
 SELECT file.file_name,
    variables.variable,
    coordinates.value AS depth,
    l_name.value AS long_name,
    s_name.value AS standard_name
   FROM ((((public.file
     LEFT JOIN public.variables USING (file_id))
     LEFT JOIN public.variables_attributes l_name ON (((l_name.file_id = file.file_id) AND ((variables.variable)::text = (l_name.variable)::text) AND ((l_name.name)::text = 'long_name'::text))))
     LEFT JOIN public.variables_attributes s_name ON (((s_name.file_id = file.file_id) AND ((variables.variable)::text = (s_name.variable)::text) AND ((s_name.name)::text = 'standard_name'::text))))
     LEFT JOIN public.coordinates ON (((coordinates.file_id = file.file_id) AND (coordinates.axis = 'Z'::bpchar))));


ALTER TABLE public.variable_names OWNER TO postgres;

--
-- Name: variable_sensor_make_model; Type: VIEW; Schema: public; Owner: postgres
--

CREATE VIEW public.variable_sensor_make_model AS
 SELECT variables.file_id,
    file.file_name,
    variables.variable,
    variables.name,
    sensor_make.value AS sensor_make,
    sensor_model.value AS sensor_model,
    sensor_sn.value AS sensor_serial_number,
    inst.make,
    inst.model,
    inst.sn
   FROM (((((public.variables
     JOIN public.file USING (file_id))
     LEFT JOIN public.variables_attributes sensor_make ON (((sensor_make.file_id = variables.file_id) AND ((sensor_make.variable)::text = (variables.variable)::text) AND ((sensor_make.name)::text = 'sensor_manufacturer'::text))))
     LEFT JOIN public.variables_attributes sensor_model ON (((sensor_model.file_id = variables.file_id) AND ((sensor_model.variable)::text = (variables.variable)::text) AND (((sensor_model.name)::text = 'sensor_model'::text) OR ((sensor_model.name)::text = 'sensor_name'::text)))))
     LEFT JOIN public.variables_attributes sensor_sn ON (((sensor_sn.file_id = variables.file_id) AND ((sensor_sn.variable)::text = (variables.variable)::text) AND ((sensor_sn.name)::text = 'sensor_serial_number'::text))))
     LEFT JOIN ( SELECT variables_attributes.file_id,
            variables_attributes.variable,
            inst_make.value AS make,
            inst_model.value AS model,
            inst_sn.value AS sn
           FROM (((public.variables_attributes
             LEFT JOIN public.variables_attributes inst_make ON (((inst_make.file_id = variables_attributes.file_id) AND ((inst_make.variable)::text = (variables_attributes.value)::text) AND ((inst_make.name)::text = 'manufacturer'::text))))
             LEFT JOIN public.variables_attributes inst_model ON (((inst_model.file_id = variables_attributes.file_id) AND ((inst_model.variable)::text = (variables_attributes.value)::text) AND ((inst_model.name)::text = 'model'::text))))
             LEFT JOIN public.variables_attributes inst_sn ON (((inst_sn.file_id = variables_attributes.file_id) AND ((inst_sn.variable)::text = (variables_attributes.value)::text) AND ((inst_sn.name)::text = 'SN'::text))))
          WHERE ((variables_attributes.name)::text = 'instrument'::text)) inst ON (((variables.file_id = inst.file_id) AND ((variables.variable)::text = (inst.variable)::text))))
  WHERE ((sensor_model.value IS NOT NULL) OR (sensor_make.* IS NOT NULL) OR (inst.variable IS NOT NULL));


ALTER TABLE public.variable_sensor_make_model OWNER TO postgres;

--
-- Name: file file_pk; Type: CONSTRAINT; Schema: public; Owner: ubuntu
--

ALTER TABLE ONLY public.file
    ADD CONSTRAINT file_pk PRIMARY KEY (file_id);


--
-- Name: file file_unique_file; Type: CONSTRAINT; Schema: public; Owner: ubuntu
--

ALTER TABLE ONLY public.file
    ADD CONSTRAINT file_unique_file UNIQUE (file_name, date_created);


--
-- Name: index_file index_file_pk; Type: CONSTRAINT; Schema: public; Owner: pete
--

ALTER TABLE ONLY public.index_file
    ADD CONSTRAINT index_file_pk PRIMARY KEY (file_id);


--
-- Name: index_file index_file_unique_file; Type: CONSTRAINT; Schema: public; Owner: pete
--

ALTER TABLE ONLY public.index_file
    ADD CONSTRAINT index_file_unique_file UNIQUE (file_name);


--
-- Name: variables_attributes variables_attribute_pk; Type: CONSTRAINT; Schema: public; Owner: ubuntu
--

ALTER TABLE ONLY public.variables_attributes
    ADD CONSTRAINT variables_attribute_pk PRIMARY KEY (file_id, variable, name);


--
-- Name: variables variables_pk; Type: CONSTRAINT; Schema: public; Owner: ubuntu
--

ALTER TABLE ONLY public.variables
    ADD CONSTRAINT variables_pk PRIMARY KEY (file_id, variable);


--
-- Name: file_id_index; Type: INDEX; Schema: public; Owner: ubuntu
--

CREATE INDEX file_id_index ON public.file USING btree (file_id);


--
-- Name: fki_coordinates_fk; Type: INDEX; Schema: public; Owner: ubuntu
--

CREATE INDEX fki_coordinates_fk ON public.coordinates USING btree (file_id);


--
-- Name: fki_global_attributes_fk; Type: INDEX; Schema: public; Owner: ubuntu
--

CREATE INDEX fki_global_attributes_fk ON public.global_attributes USING btree (file_id);


--
-- Name: fki_variables_attributes_fk_file; Type: INDEX; Schema: public; Owner: ubuntu
--

CREATE INDEX fki_variables_attributes_fk_file ON public.variables_attributes USING btree (file_id);


--
-- Name: global_attributes_index; Type: INDEX; Schema: public; Owner: ubuntu
--

CREATE INDEX global_attributes_index ON public.global_attributes USING btree (file_id, name);


--
-- Name: variable_attribute_index; Type: INDEX; Schema: public; Owner: ubuntu
--

CREATE INDEX variable_attribute_index ON public.variables_attributes USING btree (file_id, variable);


--
-- Name: variable_index; Type: INDEX; Schema: public; Owner: ubuntu
--

CREATE INDEX variable_index ON public.variables USING btree (file_id);


--
-- Name: variable_name_index; Type: INDEX; Schema: public; Owner: ubuntu
--

CREATE INDEX variable_name_index ON public.variables USING btree (name);


--
-- Name: coordinates coordinates_fk; Type: FK CONSTRAINT; Schema: public; Owner: ubuntu
--

ALTER TABLE ONLY public.coordinates
    ADD CONSTRAINT coordinates_fk FOREIGN KEY (file_id) REFERENCES public.file(file_id) NOT VALID;


--
-- Name: file file_fk_file_id; Type: FK CONSTRAINT; Schema: public; Owner: ubuntu
--

ALTER TABLE ONLY public.file
    ADD CONSTRAINT file_fk_file_id FOREIGN KEY (file_id) REFERENCES public.index_file(file_id) NOT VALID;


--
-- Name: global_attributes global_attributes_fk; Type: FK CONSTRAINT; Schema: public; Owner: ubuntu
--

ALTER TABLE ONLY public.global_attributes
    ADD CONSTRAINT global_attributes_fk FOREIGN KEY (file_id) REFERENCES public.file(file_id) NOT VALID;


--
-- Name: variables variable_fk_file; Type: FK CONSTRAINT; Schema: public; Owner: ubuntu
--

ALTER TABLE ONLY public.variables
    ADD CONSTRAINT variable_fk_file FOREIGN KEY (file_id) REFERENCES public.file(file_id) NOT VALID;


--
-- Name: variables_attributes variables_attributes_fk_file; Type: FK CONSTRAINT; Schema: public; Owner: ubuntu
--

ALTER TABLE ONLY public.variables_attributes
    ADD CONSTRAINT variables_attributes_fk_file FOREIGN KEY (file_id, variable) REFERENCES public.variables(file_id, variable) NOT VALID;


--
-- Name: SCHEMA public; Type: ACL; Schema: -; Owner: postgres
--

GRANT ALL ON SCHEMA public TO ubuntu;


--
-- Name: TABLE coordinates; Type: ACL; Schema: public; Owner: ubuntu
--

GRANT SELECT ON TABLE public.coordinates TO readonly;


--
-- Name: TABLE file; Type: ACL; Schema: public; Owner: ubuntu
--

GRANT SELECT ON TABLE public.file TO readonly;


--
-- Name: TABLE global_attributes; Type: ACL; Schema: public; Owner: ubuntu
--

GRANT SELECT ON TABLE public.global_attributes TO readonly;


--
-- Name: TABLE variables; Type: ACL; Schema: public; Owner: ubuntu
--

GRANT SELECT ON TABLE public.variables TO readonly;


--
-- Name: TABLE variables_attributes; Type: ACL; Schema: public; Owner: ubuntu
--

GRANT SELECT ON TABLE public.variables_attributes TO readonly;


--
-- Name: TABLE variable_names; Type: ACL; Schema: public; Owner: postgres
--

GRANT SELECT ON TABLE public.variable_names TO readonly;


--
-- Name: TABLE variable_sensor_make_model; Type: ACL; Schema: public; Owner: postgres
--

GRANT SELECT ON TABLE public.variable_sensor_make_model TO readonly;


--
-- PostgreSQL database dump complete
--

