
inline void chisel_test_filter(Registration::Reader& r, bool& match) {
    int64_t ts = r.read_timestamp(); (void)ts;

    r.read_readings().for_each([&](Registration::Item::Reader& item) {
        item.skip_timestamp();
        auto st = item.read_sensor_type();
        if (!st.empty() && (st[0] == 'A' || st[0] == 'a')) match = true;

        item.skip_remaining();

        return !match;
    });

    r.skip_remaining();
}
