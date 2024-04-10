#include <curl/curl.h>
#include "schema.hh"
#include "relmodel.hh"
#include "dut.hh"
#include <cstring>
#include <string>
#include <sstream>
#include <unistd.h>
#include <iostream>

typedef size_t (*callback)(char*, size_t, size_t , void*);

class general {
  public:
     static size_t delimiter(std::string line);
};

struct connect_fb {

  connect_fb(const char* URI, bool schema=0); // checks for connection to remote FB server
  CURLcode exec_query(const char* URI,const char* query, callback func, callback header); // wrapper call to FB to execute various IS queries
  CURL* curl=NULL;
  char* fb_url=nullptr;
};



struct schema_fb : schema, connect_fb, general{
    schema_fb(std::string URI); 
    static size_t table_callback(char* ptr, size_t size, size_t resp_size, void* userdata); // callback function to load table info (names)
    static size_t col_callback(char* ptr, size_t size, size_t resp_size, void* userdata); // callback function to load column info (names, data-type)
    virtual std::string quote_name(const std::string &id) {
    return id;
  }
};


struct dut_fb : dut_base, connect_fb, general {
  void test(const std::string &stmt) override;
  static size_t dut_callback(char* ptr, size_t size, size_t resp_size, void* userdata);
  static size_t header_callback(char* buffer, size_t size, size_t nitems, void* userdata);
  dut_fb(std::string URI);
};
