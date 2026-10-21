#include <taskforge/database.hpp>
#include <taskforge/runtime.hpp>
#include <iostream>
int main(){
 try{
  taskforge::Database database(taskforge::environment("DATABASE_URL"));
  std::cout<<"connected"<<std::endl;
  std::cin.get();
  try{database.query("SELECT 1");return 2;}
  catch(const std::exception& error){return std::string(error.what()).find("deadline")!=std::string::npos?0:3;}
 }catch(const std::exception&){return 4;}
}
