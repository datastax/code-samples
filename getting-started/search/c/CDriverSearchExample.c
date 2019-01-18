#include <dse.h>
#include <stdio.h>

void execute_query(CassSession* session, const char* query) {
  CassError rc = CASS_OK;
  CassStatement* statement = cass_statement_new(query, 0);
  CassFuture* future = cass_session_execute(session, statement);
  cass_future_wait(future);
  cass_future_free(future);
  cass_statement_free(statement);
}

void prepare_insert(CassSession* session, const CassPrepared** prepared) {
  CassFuture* future = NULL;
  const char* insert_stmt = "INSERT INTO world_ks.world_search_tbl (continent, country, capital, location) VALUES (?,?,?,?)";
  future = cass_session_prepare(session, insert_stmt);
  cass_future_wait(future);
  *prepared = cass_future_get_prepared(future);
  cass_future_free(future);
}

void insert_stmt(CassSession* session, const CassPrepared* prepared, const char* continent, const char* country, const char* capital, const float latitude, const float longitude){

    CassStatement* statement = cass_prepared_bind(prepared);
    cass_statement_bind_string(statement, 0, continent);
    cass_statement_bind_string(statement, 1, country);
    cass_statement_bind_string(statement, 2, capital);
    cass_statement_bind_dse_point(statement, 3, latitude, longitude);
    CassFuture* future = cass_session_execute(session, statement);
    cass_future_wait(future);
    cass_future_free(future);
    cass_statement_free(statement);
}

int main() {

  CassFuture* connect_future = NULL;
  CassCluster* cluster = cass_cluster_new();
  CassSession* session = cass_session_new();

  cass_cluster_set_contact_points(cluster, "127.0.0.1");
  connect_future = cass_session_connect(session, cluster);

  if (cass_future_error_code(connect_future) != CASS_OK) {
    cass_future_free(connect_future);
    cass_cluster_free(cluster);
    cass_session_free(session);
    return -1;
  }

  const char* create_keyspace_stmt = "CREATE KEYSPACE IF NOT EXISTS world_ks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'};";
  const char* create_table_stmt = "CREATE TABLE IF NOT EXISTS world_ks.world_search_tbl (continent text, country text, capital text, location 'PointType', PRIMARY KEY (continent, country));";
  const char* create_search_index_stmt = "CREATE SEARCH INDEX IF NOT EXISTS ON world_ks.world_search_tbl WITH COLUMNS location;";

  execute_query(session, create_keyspace_stmt);
  execute_query(session, create_table_stmt);
  execute_query(session, create_search_index_stmt);

  const CassPrepared* prepared = NULL;
  prepare_insert(session, &prepared);
  insert_stmt(session, prepared, "Europe", "United Kingdom", "London", 51.5074, 0.1278);
  insert_stmt(session, prepared, "Europe", "Germany", "Berlin", 52.5200, 13.4050);
  insert_stmt(session, prepared, "Europe", "Spain", "Madrid", 40.4168, 3.7038);
  insert_stmt(session, prepared, "Asia", "China", "Beijing", 39.9042, 116.4074);
  insert_stmt(session, prepared, "North America", "United States", "Washington DC", 38.9072, 77.0369);
  cass_prepared_free(prepared);

  const char* solr_query = "{ \"q\":\"*:*\", \"fq\":\"location:\\\"IsWithin(BUFFER(POINT(45 0), 20.0))\\\"\" }";
  CassStatement* read_stmt = cass_statement_new("SELECT country, capital FROM world_ks.world_search_tbl WHERE solr_query = '{ \"q\":\"*:*\", \"fq\":\"location:\\\"IsWithin(BUFFER(POINT(45 0), 20.0))\\\"\" }'", 0);
  CassFuture* read_future = cass_session_execute(session, read_stmt);

  cass_future_wait(read_future);

  const CassResult* result = cass_future_get_result(read_future);
  CassIterator* iterator = cass_iterator_from_result(result);

  while (cass_iterator_next(iterator)) {
    const CassRow* row = cass_iterator_get_row(iterator);
    const CassValue* country = cass_row_get_column_by_name(row, "country");
    const CassValue* capital = cass_row_get_column_by_name(row, "capital");
    const char* country_char;
    size_t country_length;
    const char* capital_char;
    size_t capital_length;
    cass_value_get_string(country, &country_char, &country_length);
    cass_value_get_string(capital, &capital_char, &capital_length);
    printf("The capital of European country %.*s is %.*s\n", (int)country_length, country_char, (int)capital_length, capital_char);
  }

  cass_result_free(result);
  cass_iterator_free(iterator);

  CassFuture* close_future = cass_session_close(session);
  cass_future_wait(close_future);
  cass_future_free(close_future);

  cass_cluster_free(cluster);
  cass_session_free(session);

  return 0;
}