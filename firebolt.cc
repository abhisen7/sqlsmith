#include "firebolt.hh"
#include <regex>

char *fb_err_resp = nullptr;

static std::regex re_header_200("200 OK");
static std::regex re_syntax_err("syntax error,");
static std::regex re_err_resp(": ");

int c = 0;

std::string IS_query_table =
    "select table_name,table_type from information_schema.tables where "
    "table_type == 'BASE TABLE';"; // Only deal with DIMENSION/FACT table
                                   // schemas
std::string IS_query_column = "select column_name,data_type from "
                              "information_schema.columns where table_name = ";
std::vector<table> tab_; // we use this vector first to push the table info, and
                         // then copy the elements to schema::tables vector
std::vector<column>
    col_; // we use this vector first to push the column info, and then copy the
          // elements to schema::tables.cols vector
char header[CURL_MAX_HTTP_HEADER];

size_t general::delimiter(std::string line) {
  const char *delim = "\t";
  size_t pos = line.find(delim, 0);
  return pos;
}

connect_fb::connect_fb(const char *URI, bool schema) {
  if (schema == 0) {
    fb_url = static_cast<char *>(malloc(strlen(URI)));
    strcpy(fb_url, URI);
  }
  std::cerr << "\n\n\n***************************************************"
               "\n\n\nChecking connection to Firebolt "
               "server...\n\n\n************************************************"
               "***\n\n\n"
            << std::endl;
  curl = curl_easy_init();
  if (curl) {
    CURLcode ret;
    curl_easy_setopt(curl, CURLOPT_URL, URI);
    //  curl_easy_setopt(curl, CURLOPT_BUFFERSIZE, 102400L);
    curl_easy_setopt(curl, CURLOPT_MAXREDIRS, 50L);
    curl_easy_setopt(curl, CURLOPT_HTTP_VERSION, (long)CURL_HTTP_VERSION_2TLS);
    //  curl_easy_setopt(curl, CURLOPT_FTP_SKIP_PASV_IP, 1L);
    // curl_easy_setopt(curl, CURLOPT_TCP_KEEPALIVE, 1L);

    ret = curl_easy_perform(curl);
    try {
      if (ret != CURLE_OK) {
        throw dut::broken("Connection cannot be established..");
      }
    } catch (dut::broken &e) {
      throw;
    }

    std::cerr << "Connected to "
                 "Firebolt...\n\n\n********************************************"
                 "*******\n\n\n";
  }
}

CURLcode connect_fb::exec_query(const char *URI, const char *query,
                                callback func, callback header) {
  CURLcode ret;
  std::string str(query);
  size_t post_size = str.size();
  curl_easy_setopt(curl, CURLOPT_BUFFERSIZE, 102400L);
  curl_easy_setopt(curl, CURLOPT_URL, URI);
  curl_easy_setopt(curl, CURLOPT_NOPROGRESS, 1L);
  curl_easy_setopt(curl, CURLOPT_POSTFIELDS, query);
  curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE_LARGE, (curl_off_t)post_size);
  curl_easy_setopt(curl, CURLOPT_MAXREDIRS, 50L);
  curl_easy_setopt(curl, CURLOPT_HTTP_VERSION, (long)CURL_HTTP_VERSION_2TLS);
  // curl_easy_setopt(curl, CURLOPT_FTP_SKIP_PASV_IP, 1L);
  // curl_easy_setopt(curl, CURLOPT_TCP_KEEPALIVE, 1L);
  curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, func);
  curl_easy_setopt(curl, CURLOPT_WRITEDATA, NULL);
  curl_easy_setopt(curl, CURLOPT_HEADERFUNCTION, header);
  curl_easy_setopt(curl, CURLOPT_HEADERDATA, NULL);

  ret = curl_easy_perform(curl);
  return ret;
}

size_t schema_fb::table_callback(char *ptr, size_t size, size_t resp_size,
                                 void *userdata) {

  std::string curl_response(ptr, resp_size - 1);
  std::string line;
  std::stringstream ss(curl_response);

  while (std::getline(ss, line)) {
    std::string p;
    size_t position = delimiter(line);

    p = line.substr(0, position);

    table tab(p, "public", true, true);
    tab_.push_back(tab);
  }

  return resp_size;
}

size_t schema_fb::col_callback(char *ptr, size_t size, size_t resp_size,
                               void *userdata) {

  std::string line;
  std::string col_schema(ptr, resp_size - 1);
  std::stringstream ss(col_schema);

  while (std::getline(ss, line)) {
    std::pair<std::string, std::string> p;
    size_t position = delimiter(line);

    std::stringstream _ss_col_name;
    std::stringstream _ss_col_type;
    size_t position_ = 0;

    p.first = line.substr(0, position);

    for (int j = position; j <= line.length(); j++) {
      if (!isspace(line.c_str()[j])) {
        position_ = j;
        break;
      }
    }

    p.second = line.substr(position_, line.length());
    std::cerr << p.first << "," << p.second << "\n";

    column col(p.first, sqltype::get(p.second));
    col_.push_back(col);
  }
  return resp_size;
}

schema_fb::schema_fb(std::string URI) : connect_fb(URI.c_str(), 1) {

  std::cerr << "\nLoading table schemas... "
               "\n\n\n***************************************************\n";
  exec_query(URI.c_str(), IS_query_table.c_str(), table_callback, NULL);
  for (int l = 0; l < tab_.size(); l++) {
    tables.push_back(tab_[l]);
  }

  std::cerr << "Loading column schemas... "
               "\n\n\n###############################################\n";

  for (int l = 0; l < tables.size(); l++) {

    std::string query = IS_query_column + "'" + tables[l].name + "'" + ";";
    std::cerr << "\nLoading columns for table: " << tables[l].name << "\n";
    exec_query(URI.c_str(), query.c_str(), col_callback, NULL);
    for (int k = 0; k < col_.size(); k++) {
      tables[l].columns().push_back(col_[k]);
      std::cerr << "\nColumn Name: " << tables[l].cols[k].name
                << ", Column Type: " << tables[l].cols[k].type->name << "\n";
    }
    std::cerr << "\n\n\n";

    col_.clear();
  }

#define BINOP(n, t)                                                            \
  do {                                                                         \
    op o(#n, sqltype::get(#t), sqltype::get(#t), sqltype::get(#t));            \
    register_operator(o);                                                      \
  } while (0)

  BINOP(*, INTEGER);
  BINOP(/, INTEGER);
  BINOP(+, INTEGER);
  BINOP(-, INTEGER);
  BINOP(%, INTEGER);
  BINOP(^, INTEGER);

  /*BINOP(>>, INTEGER);
  BINOP(<<, INTEGER);

  BINOP(&, INTEGER);
  BINOP(|, INTEGER); */

  BINOP(!=, INTEGER);
  BINOP(<, INTEGER);
  BINOP(<=, INTEGER);
  BINOP(>, INTEGER);
  BINOP(>=, INTEGER);
  BINOP(=, INTEGER);
  BINOP(<>, INTEGER);

  BINOP(IS, INTEGER);
  BINOP(IS NOT, INTEGER);

  BINOP(AND, INTEGER);
  BINOP(AND, TEXT);

  BINOP(OR, INTEGER);
  BINOP(OR, TEXT);

  BINOP(NOT, INTEGER);
  BINOP(NOT, TEXT);
  BINOP(||, TEXT);

  /* Add functions listed under
   * https://docs.firebolt.io/sql-reference/functions-reference/#sql-functions
   */

#define FUNC(n, r)                                                             \
  do {                                                                         \
    routine proc("", "", sqltype::get(#r), #n);                                \
    register_routine(proc);                                                    \
  } while (0)

#define FUNC1(n, r, a)                                                         \
  do {                                                                         \
    routine proc("", "", sqltype::get(#r), #n);                                \
    proc.argtypes.push_back(sqltype::get(#a));                                 \
    register_routine(proc);                                                    \
  } while (0)

#define FUNC2(n, r, a, b)                                                      \
  do {                                                                         \
    routine proc("", "", sqltype::get(#r), #n);                                \
    proc.argtypes.push_back(sqltype::get(#a));                                 \
    proc.argtypes.push_back(sqltype::get(#b));                                 \
    register_routine(proc);                                                    \
  } while (0)

#define FUNC3(n, r, a, b, c)                                                   \
  do {                                                                         \
    routine proc("", "", sqltype::get(#r), #n);                                \
    proc.argtypes.push_back(sqltype::get(#a));                                 \
    proc.argtypes.push_back(sqltype::get(#b));                                 \
    proc.argtypes.push_back(sqltype::get(#c));                                 \
    register_routine(proc);                                                    \
  } while (0)

  // FUNC(last_insert_rowid, INTEGER);

  // Firebolt String Functions
  FUNC(random, INTEGER);
  FUNC(gen_random_uuid, TEXT);

  FUNC1(base64_encode, TEXT, TEXT);

  FUNC2(ltrim, TEXT, TEXT, TEXT);
  FUNC2(rtrim, TEXT, TEXT, TEXT);
  FUNC2(trim, TEXT, TEXT, TEXT);

  /*FUNC(sqlite_source_id, TEXT);
  FUNC(sqlite_version, TEXT);
  FUNC(total_changes, INTEGER);*/

  FUNC1(acos, DOUBLE PRECISION, INTEGER);
  FUNC1(acos, DOUBLE PRECISION, REAL);
  FUNC1(atan, DOUBLE PRECISION, INTEGER);
  FUNC1(atan, INTEGER, REAL);
  FUNC1(asin, DOUBLE PRECISION, INTEGER);
  FUNC1(asin, DOUBLE PRECISION, REAL);
  FUNC1(abs, INTEGER, REAL);
  FUNC1(abs, INTEGER, REAL);
  FUNC1(sign, DOUBLE PRECISION, INTEGER);

  FUNC1(sqrt, DOUBLE PRECISION,
        INTEGER); // to-do: Add NULL ret value for negative arg

  FUNC2(log, DOUBLE PRECISION, INTEGER,
        INTEGER); // to-do: Test with positive real numbers as base and exponent
                  // for arguments
  FUNC2(round, DOUBLE PRECISION, REAL, INTEGER);
  FUNC2(pow, DOUBLE PRECISION, REAL, REAL);

  /*FUNC1(hex, TEXT, TEXT);
  FUNC1(length, INTEGER, TEXT);
  FUNC1(lower, TEXT, TEXT);
  FUNC1(ltrim, TEXT, TEXT);
  FUNC1(quote, TEXT, TEXT);
  FUNC1(randomblob, TEXT, INTEGER);
  FUNC1(round, INTEGER, REAL);
  FUNC1(rtrim, TEXT, TEXT);
  FUNC1(soundex, TEXT, TEXT);
  FUNC1(sqlite_compileoption_get, TEXT, INTEGER);
  FUNC1(sqlite_compileoption_used, INTEGER, TEXT);
  FUNC1(trim, TEXT, TEXT);
  FUNC1(typeof, TEXT, INTEGER);
  FUNC1(typeof, TEXT, NUMERIC);
  FUNC1(typeof, TEXT, REAL);
  FUNC1(typeof, TEXT, TEXT);
  FUNC1(unicode, INTEGER, TEXT);
  FUNC1(upper, TEXT, TEXT);
  FUNC1(zeroblob, TEXT, INTEGER);

  FUNC2(glob, INTEGER, TEXT, TEXT);
  FUNC2(instr, INTEGER, TEXT, TEXT);
  FUNC2(like, INTEGER, TEXT, TEXT);


  FUNC2(round, INTEGER, REAL, INTEGER);
  FUNC2(substr, TEXT, TEXT, INTEGER);

  FUNC3(substr, TEXT, TEXT, INTEGER, INTEGER);
  FUNC3(replace, TEXT, TEXT, TEXT, TEXT);*/

  /* Aggregate Functions */
  // To-do: Expand AGG macro to functions with 2 arguments (e.g. MAX_BY,
  // PERCENTILE_CONT) and array functions (ARRAY_AGG)

#define AGG(n, r, a)                                                           \
  do {                                                                         \
    routine proc("", "", sqltype::get(#r), #n);                                \
    proc.argtypes.push_back(sqltype::get(#a));                                 \
    register_aggregate(proc);                                                  \
  } while (0)

  AGG(any, INTEGER, INTEGER);
  AGG(any, REAL, REAL);
  AGG(any, TEXT, TEXT);
  AGG(avg, INTEGER, INTEGER);
  AGG(avg, REAL, REAL);
  AGG(count, INTEGER, REAL);
  AGG(count, INTEGER, TEXT);
  AGG(count, INTEGER, INTEGER);
  //AGG(group_concat, TEXT, TEXT);
  AGG(max, REAL, REAL);
  AGG(max, INTEGER, INTEGER);
  AGG(sum, REAL, REAL);
  AGG(sum, INTEGER, INTEGER);
  AGG(total, REAL, INTEGER);
  AGG(total, REAL, REAL);
  AGG(min, INTEGER, INTEGER);
  AGG(median, INTEGER, INTEGER);
  AGG(checksum, INTEGER, INTEGER);
  AGG(checksum, INTEGER, TEXT);
  AGG(checksum, INTEGER, REAL);

  /* Binary Functions */

#define BIN1(n, r, a)                                                          \
  do {                                                                         \
    routine proc("", "", sqltype::get(#r), #n);                                \
    proc.argtypes.push_back(sqltype::get(#a));                                 \
    register_aggregate(proc);                                                  \
  } while (0)

#define BIN2(n, r, a, b)                                                       \
  do {                                                                         \
    routine proc("", "", sqltype::get(#r), #n);                                \
    proc.argtypes.push_back(sqltype::get(#a));                                 \
    proc.argtypes.push_back(sqltype::get(#b));                                 \
    register_aggregate(proc);                                                  \
  } while (0)

  BIN1(concat, TEXT, TEXT);
  BIN1(length, INTEGER, TEXT);
  BIN1(octet_length, BIGINT, TEXT);
  BIN2(decode, BYTEA, TEXT, HEX);
  BIN2(decode, BYTEA, TEXT, BASE64);
  BIN2(decode, BYTEA, TEXT, ESCAPE);
  BIN2(encode, TEXT, BYTEA, HEX);
  BIN2(encode, TEXT, BYTEA, BASE64);
  BIN2(encode, TEXT, BYTEA, ESCAPE);

  booltype = sqltype::get("INTEGER");
  inttype = sqltype::get("INTEGER");

  internaltype = sqltype::get("internal"); /// Not sure what internal type means
  arraytype = sqltype::get("ARRAY");

  true_literal = "1";
  false_literal = "0";

  generate_indexes();
  curl_easy_cleanup(
      curl); // tear down curl connection until DUT is initialized again
  curl = nullptr;
}

size_t dut_fb::dut_callback(char *ptr, size_t size, size_t resp_size,
                            void *userdata) {
  (void)size;
  (void)userdata;
  fb_err_resp = static_cast<char *>(malloc(resp_size + 1));
  std::memcpy(fb_err_resp, ptr, resp_size);
  fb_err_resp[resp_size] = '\0';
  std::stringstream _error_(fb_err_resp);
  std::string _error;
  while (std::getline(_error_, _error)) {
    std::smatch regex_match;
    std::regex_search(_error, regex_match, re_err_resp);
    size_t position = regex_match.prefix().length();
    std::string substring_temp = _error.substr(position + 2);
    std::strcpy(fb_err_resp, substring_temp.c_str());
    break;
  }
  return resp_size;
}

size_t dut_fb::header_callback(char *buffer, size_t size, size_t nitems,
                               void *userdata) {
  if (c > 0) {
    (void)buffer;
    (void)size;
    (void)userdata;
    return nitems;
  }
  /*buffer is not null-terminated and has an additional new-line at the end of
   * each header line, nitems includes the new-line*/
  buffer[nitems - 1] = '\0';
  strcpy(header, buffer);
  c++;
  return nitems;
}

dut_fb::dut_fb(std::string URI) : connect_fb(URI.c_str(), 0) {
  std::cerr << "\n\n\nStarting DUT testing...\n\n\n";
}

void dut_fb::test(const std::string &statement) {

  header[0] = '\0';
  exec_query(fb_url, statement.c_str(), dut_callback, header_callback);
  if (!regex_search(header, re_header_200)) {
    try {
      if (std::regex_search(fb_err_resp, re_syntax_err)) {
        throw dut::syntax(fb_err_resp);
      } else {
        throw dut::failure(fb_err_resp);
        return;
      }
    } catch (dut::failure &e) {
      throw;
    }
  }
  curl_easy_cleanup(curl);
  curl = NULL;
  free(fb_err_resp);
  free(fb_url);
}