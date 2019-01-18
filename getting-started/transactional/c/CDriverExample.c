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
  const char* insert_stmt = "INSERT INTO world_ks.world_tbl (continent, country, capital) VALUES (?,?,?)";
  future = cass_session_prepare(session, insert_stmt);
  cass_future_wait(future);
  *prepared = cass_future_get_prepared(future);
  cass_future_free(future);
}

void insert_stmt(CassSession* session, const CassPrepared* prepared, const char* continent, const char* country, const char* capital){

    CassStatement* statement = cass_prepared_bind(prepared);
    cass_statement_bind_string(statement, 0, continent);
    cass_statement_bind_string(statement, 1, country);
    cass_statement_bind_string(statement, 2, capital);
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
  const char* create_table_stmt = "CREATE TABLE IF NOT EXISTS world_ks.world_tbl (continent text, country text, capital text, PRIMARY KEY (continent, country))";

  execute_query(session, create_keyspace_stmt);
  execute_query(session, create_table_stmt);

  const CassPrepared* prepared = NULL;
  prepare_insert(session, &prepared);
  insert_stmt(session, prepared, "Europe", "United Kingdom", "London");
  insert_stmt(session, prepared, "Europe", "Germany", "Berlin");
  insert_stmt(session, prepared, "Europe", "Spain", "Madrid");
  insert_stmt(session, prepared, "Asia", "China", "Beijing");
  insert_stmt(session, prepared, "North America", "United States", "Washington DC");
  cass_prepared_free(prepared);

  CassStatement* read_stmt = cass_statement_new("SELECT country, capital FROM world_ks.world_tbl WHERE continent = 'Europe'", 0);
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