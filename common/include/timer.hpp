#pragma once

#include <chrono>

class Timer
{
public:
    using Clock = std::chrono::steady_clock;

    Timer() : start_(Clock::now()) {}

    void reset()
    {
        start_ = Clock::now();
    }

    double elapsed_ms() const
    {
        return std::chrono::duration<double, std::milli>(Clock::now() - start_).count();
    }

private:
    Clock::time_point start_;
};
