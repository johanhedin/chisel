
inline void chisel_test_filter(Registration::Reader& r, bool& match) {
    int64_t ts = r.read_timestamp(); (void)ts;

    r.read_readings().for_each([&](Registration::Item::Reader& item) {
        item.skip_timestamp();
        auto st = item.read_sensor_type();
        if (!st.empty() && (st[0] == 'A' || st[0] == 'a')) match = true;

        // skip_remaining() is not needed since it is handled under the hood
        //item.skip_remaining();

        return !match;
    });

    r.skip_extra_readings();

    auto optional_readings = r.read_optional_readings();
    if (optional_readings.has_value()) {
        optional_readings.for_each([&](Registration::Item::Reader& item) {
            item.skip_timestamp();
            auto st = item.read_sensor_type();
            if (!st.empty() && (st[0] == 'D' || st[0] == 'd')) match = true;

            // skip_remaining() is not needed since it is handled under the hood
            //item.skip_remaining();

            return !match;
        });
    }

    r.skip_remaining();
}
