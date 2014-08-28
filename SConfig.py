# THIS FILE WAS AUTOMATICALLY GENERATED. MANUAL CHANGES IN THIS FILE WILL BE LOST AFTER THE NEXT C/C++ PROJECT CHANGES!!!
SCONS_OPTIONS = {'num_jobs':'8'}
DECIDER = 'MD5'
COMP_INCLUDES_INTO_CCFLAGS = False
BUILD_ARTIFACT_NAME = 'mload'
PROJECT_TYPE = 'exe'
BUILD_CONFIGURATION = 'Release'
TOOLCHAIN_NAME = 'linux gcc'
C_FLAGS = '-O3 -Wall -c -fmessage-length=0 -fomit-frame-pointer'
CXX_FLAGS = '-std=c++1y -I/usr/include/libbson-1.0 -I/usr/include/libmongoc-1.0 -O3 -Wall -c -fmessage-length=0 -fomit-frame-pointer'
COMPILER_NAME = 'g++'
PROJECT_NAME = 'mload'
INCLUDES = ['/usr/include/libbson-1.0', '/usr/include/libmongoc-1.0']
LIBRARIES = ['tcmalloc', 'mongoclient', 'boost_program_options', 'mongoc-1.0', 'bson-1.0', 'boost_system', 'boost_regex', 'boost_thread', 'boost_chrono', 'boost_filesystem']
LIBRARY_PATHS = []
SOURCE_PATHS = {'src':[]}
PRE_BUILD_COMMAND = ''
PRE_BUILD_DESC = ''
POST_BUILD_COMMAND = ''
POST_BUILD_DESC = ''
COMPILER_DEFINES = {}
LINKER_FLAGS = ''
