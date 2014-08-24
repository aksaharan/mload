/*    Copyright Charlie Page 2014
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

#include <iostream>
#include <mongo/client/dbclient.h>
#include <mongoc.h>
#include "loader.h"
#include "tools.h"
#include "programoptions.h"

#include "index.h"

int main(int argc, char* argv[]) {
    cpp::SimpleTimer<> totalTimer;
    totalTimer.start();
    //C++ Driver
    mongo::client::initialize();
    //TODO: remove mongoc_init when the c driver is removed from options
    //C Driver
    mongoc_init();

    //Program beings: read settings, then hand those settings off to the loader.
    loader::Loader::Settings settings;
    loader::setProgramOptions(settings, argc, argv);

    //C++ driver, remove the cdriver requirements.
    settings.connection = settings.connection.substr(std::string("mongodb://").size());

    std::cout << "init finished" << std::endl;

    cpp::SimpleTimer<> timerLoad;

    //Timer for only the load
    timerLoad.start();

    //The actual loading
    loader::Loader loader(settings);
    loader.run();

    //End all timers
    timerLoad.stop();
    totalTimer.stop();

    long totalSeconds = totalTimer.seconds();
    long loadSeconds = timerLoad.seconds();

    std::cout << "\nTotal time: " << totalSeconds / 60 << "m" << totalSeconds % 60 << "s"
              << "\nLoad time: " << loadSeconds / 60 << "m" << loadSeconds % 60 << "s" << std::endl;

}
