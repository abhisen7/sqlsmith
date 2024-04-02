#include "firebolt.hh"
#include <sstream>
#include <cstring>
#include <string>

std::string IS_query_table= "select table_name,table_type from information_schema.tables where table_type != 'VIEW';"; // Only deal with DIMENSION/FACT table schemas
std::string IS_query_column= "select column_name,data_type from information_schema.columns where table_name = "; 
std::vector<table> tab_; // we use this vector first to push the table schema info, and then copy the elements to schema::tables vector
std::vector<column> col_;


size_t general::delimiter(std::string line){
    const char* delim="\t";
    size_t pos = line.find(delim,0);
    return pos;
}

connect_fb::connect_fb(const char* URI){
    std::cout << "\n\n\n***************************************************\n\n\nChecking connection to Firebolt server...\n\n\n***************************************************\n\n\n" << std::endl; 
    curl=curl_easy_init();
    if(curl){
        CURLcode ret;
        curl_easy_setopt(curl, CURLOPT_URL, URI);
       //  curl_easy_setopt(curl, CURLOPT_BUFFERSIZE, 102400L);
        curl_easy_setopt(curl, CURLOPT_MAXREDIRS, 50L);
        curl_easy_setopt(curl, CURLOPT_HTTP_VERSION, (long)CURL_HTTP_VERSION_2TLS);
        curl_easy_setopt(curl, CURLOPT_FTP_SKIP_PASV_IP, 1L);
        curl_easy_setopt(curl, CURLOPT_TCP_KEEPALIVE, 1L);

        ret=curl_easy_perform(curl);
        
        if(ret!=CURLE_OK){
            std::cout << "Connection not established";
            exit(1);
        }

        std::cout << "Connected to Firebolt...\n\n\n***************************************************\n\n\n";
    }
}

void schema_fb::exec_query(const char* URI, const char* query, callback func, void* u_data){
        CURLcode ret;
        std::string str(query);
        size_t post_size=str.size();
        curl_easy_setopt(curl, CURLOPT_BUFFERSIZE, 102400L);
        curl_easy_setopt(curl, CURLOPT_URL, URI);
        curl_easy_setopt(curl, CURLOPT_NOPROGRESS, 1L);
        curl_easy_setopt(curl, CURLOPT_POSTFIELDS, query);
        curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE_LARGE, (curl_off_t)post_size);
        curl_easy_setopt(curl, CURLOPT_USERAGENT, "curl/7.81.0");
        curl_easy_setopt(curl, CURLOPT_MAXREDIRS, 50L);
        curl_easy_setopt(curl, CURLOPT_HTTP_VERSION, (long)CURL_HTTP_VERSION_2TLS);
        curl_easy_setopt(curl, CURLOPT_FTP_SKIP_PASV_IP, 1L);
        curl_easy_setopt(curl, CURLOPT_TCP_KEEPALIVE, 1L);
        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, func);
        curl_easy_setopt(curl, CURLOPT_WRITEDATA, u_data);

        ret=curl_easy_perform(curl);
        
        if(ret!=CURLE_OK){
            std::cout << "Query cannot be executed";
            exit(1);
        }
}

size_t schema_fb::table_callback(char* ptr, size_t size, size_t resp_size, void* userdata){
    
    char dest[resp_size+1];
    std::memcpy(dest,ptr,resp_size);
    //dest[resp_size+1]='\0';
    std::stringstream ss;
    for (int i=0; i< resp_size-1; i++)
    {   
        std::cout << dest[i];
        ss << dest[i];
    }

    std::cout << "\n\n\n";

    size_t count=resp_size;
    

    /*std::cout << resp_size-1 << "," << strlen(dest) << "\n";
    std::cout << count << "," << resp_size << "\n";*/

    std::string line;
    int i=0;
    int j=0;

    while(std::getline(ss,line))
    {
        std::pair<std::string, std::string> p;
        size_t position= delimiter(line);
        
        std::stringstream _ss_table_name;
        std::stringstream _ss_table_type;
        size_t position_ = 0;

        for (int j=0; j< position ;j++){
            _ss_table_name << line.c_str()[j]; 
            
        }

        p.first=_ss_table_name.str(); // store the FB table_name

        for (j=position; j<=line.length();j++){
            if(!isspace(line.c_str()[j]))
            {
                position_ = j;
                break;
            }
        }

        for (int k=position_; k<=line.length();k++){
            _ss_table_type << line.c_str()[k];

        }
    
      
        p.second=_ss_table_type.str(); // store the FB table_type

        //std::cout << p.first << "," << p.second << "\n";   

        _ss_table_name.clear();
        _ss_table_type.clear();
    
        table tab(p.first,p.second,true,true);
        tab_.push_back(tab);

    }

    return count;
}

size_t schema_fb::col_callback(char* ptr, size_t size, size_t resp_size, void* userdata){

    char dest[resp_size+1];
    std::memcpy(dest,ptr,resp_size);
    //dest[resp_size+1]='\0';
    std::stringstream ss;
    for (int i=0; i< resp_size-1; i++)
    {   
        ss << dest[i];
    }
    
    std::cout << "\n";
    size_t count=resp_size;
    

    /*std::cout << resp_size-1 << "," << strlen(dest) << "\n";
    std::cout << count << "," << resp_size << "\n";*/

    std::string line;
    int i=0;
    int j=0;

    while(std::getline(ss,line))
    {
        std::pair<std::string, std::string> p;
        size_t position= delimiter(line);
        
        std::stringstream _ss_col_name;
        std::stringstream _ss_col_type;
        size_t position_ = 0;

        for (int j=0; j< position ;j++){
            _ss_col_name << line.c_str()[j]; 
            
        }

        p.first=_ss_col_name.str(); // store the FB table_name

        for (j=position; j<=line.length();j++){
            if(!isspace(line.c_str()[j]))
            {
                position_ = j;
                break;
            }
        }

        for (int k=position_; k<=line.length();k++){
            _ss_col_type << line.c_str()[k];

        }
    
      
        p.second=_ss_col_type.str(); // store the FB table_type

        //std::cout << p.first << "," << p.second << "\n";   

        _ss_col_name.clear();
        _ss_col_type.clear();
    
        column col(p.first, sqltype::get(p.second));
        col_.push_back(col);
    }

    return count;

}

schema_fb::schema_fb(std::string URI)
    :connect_fb(URI.c_str())
    {
      
        std::cout << "\nExecuting IS queries for loading table schemas... \n\n\n***************************************************\n\n\n";
        exec_query(URI.c_str(), IS_query_table.c_str(), table_callback, NULL);
        for(int l=0; l < tab_.size(); l++){
            tables.push_back(tab_[l]);
        }

        std::cout << "***************************************************\n\n\nExecuting IS queries for loading column schemas... \n\n\n***************************************************\n\n\n";

        for (int l=0; l< tables.size(); l++){
            
            std::string query= IS_query_column + "'" + tables[l].name + "'" + ";" ; 
            std::cout << "\nQuery: " << query.c_str() << "\n";
            std::cout << "\nLoading columns for table: " << tables[l].name << "\n";
            exec_query(URI.c_str(), query.c_str(), col_callback, NULL);
            for(int k=0; k < col_.size(); k++)
            {
                tables[l].columns().push_back(col_[k]);
                std::cout << "\nColumn Name: " << tables[l].cols[k].name << ", Column Type: " << tables[l].cols[k].type->name << "\n"; 
            }
            std::cout << "\n\n\n";
            col_.clear();
        }

        exit(0);
    }