#include <curl/curl.h>
#include "schema.hh"
#include "relmodel.hh"
#include "dut.hh"
#include <cstring>
#include <string>


typedef size_t (*callback)(char*, size_t, size_t , void*);

class general {
  public:
     static size_t delimiter(std::string line);
};

struct connect_fb{
  connect_fb(const char* URI); // checks for connection to remote FB server
  CURL* curl=NULL;
};



struct schema_fb : schema, connect_fb,general {
    schema_fb(std::string URI); 
    void exec_query(const char* URI,const char* query, callback func, void* userdata); // wrapper call to FB to execute various IS queries
    static size_t table_callback(char* ptr, size_t size, size_t resp_size, void* userdata); // callback function to save the table info (names, type)
    static size_t col_callback(char* ptr, size_t size, size_t resp_size, void* userdata); // callback function to save column info (names, type)
    virtual std::string quote_name(const std::string &id) {
    return id;
  }
};

