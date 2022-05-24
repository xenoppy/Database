#include "../catch.hpp"
#include <db/hello.h>
TEST_CASE("hello.h")
{
    SECTION("add") { REQUIRE(add(4) == 2); }
}