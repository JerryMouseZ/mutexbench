#pragma once

#include <cctype>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <optional>
#include <string>
#include <system_error>

namespace burn_calibration {

struct Calibration {
  uint64_t numerator = 1;
  uint64_t denominator = 1;
};

enum class LoadStatus {
  kLoaded,
  kMissingFile,
  kMissingSection,
  kError,
};

struct LoadResult {
  LoadStatus status = LoadStatus::kMissingFile;
  Calibration calibration{};
  std::filesystem::path path;
  std::string error;
};

inline std::string TrimCopy(std::string value) {
  size_t start = 0;
  while (start < value.size() &&
         std::isspace(static_cast<unsigned char>(value[start])) != 0) {
    ++start;
  }
  size_t end = value.size();
  while (end > start &&
         std::isspace(static_cast<unsigned char>(value[end - 1])) != 0) {
    --end;
  }
  return value.substr(start, end - start);
}

inline bool TryParseU64(const std::string &text, uint64_t *value) {
  try {
    size_t idx = 0;
    const unsigned long long parsed = std::stoull(text, &idx, 10);
    if (idx != text.size()) {
      return false;
    }
    *value = static_cast<uint64_t>(parsed);
    return true;
  } catch (...) {
    return false;
  }
}

inline std::filesystem::path DefaultConfigPath(const char *argv0) {
  std::error_code ec;
  std::filesystem::path executable_path =
      (argv0 != nullptr) ? std::filesystem::path(argv0) : std::filesystem::path();
  if (!executable_path.empty()) {
    if (!executable_path.is_absolute()) {
      executable_path = std::filesystem::absolute(executable_path, ec);
      if (ec) {
        executable_path.clear();
      }
    }
    if (!executable_path.empty() && executable_path.has_parent_path()) {
      return executable_path.parent_path() / "iter_calibration.cfg";
    }
  }
  const std::filesystem::path cwd = std::filesystem::current_path(ec);
  if (ec) {
    return std::filesystem::path("iter_calibration.cfg");
  }
  return cwd / "iter_calibration.cfg";
}

inline std::string ToString(const Calibration &calibration) {
  return std::to_string(calibration.numerator) + "/" +
         std::to_string(calibration.denominator);
}

inline LoadResult LoadCalibration(const std::filesystem::path &path,
                                  const std::string &section_name) {
  LoadResult result;
  result.path = path;

  std::error_code ec;
  if (!std::filesystem::exists(path, ec)) {
    result.status = LoadStatus::kMissingFile;
    return result;
  }
  if (ec) {
    result.status = LoadStatus::kError;
    result.error = "Failed to stat config file: " + path.string();
    return result;
  }

  std::ifstream input(path);
  if (!input) {
    result.status = LoadStatus::kError;
    result.error = "Failed to open config file: " + path.string();
    return result;
  }

  const std::string numerator_key = section_name + ".numerator";
  const std::string denominator_key = section_name + ".denominator";
  std::optional<uint64_t> numerator;
  std::optional<uint64_t> denominator;

  std::string line;
  size_t line_number = 0;
  while (std::getline(input, line)) {
    ++line_number;
    const std::string trimmed = TrimCopy(line);
    if (trimmed.empty() || trimmed[0] == '#' || trimmed[0] == ';') {
      continue;
    }

    const size_t equals = trimmed.find('=');
    if (equals == std::string::npos) {
      result.status = LoadStatus::kError;
      result.error = "Invalid config line " + std::to_string(line_number) +
                     " in " + path.string();
      return result;
    }

    const std::string key = TrimCopy(trimmed.substr(0, equals));
    const std::string value = TrimCopy(trimmed.substr(equals + 1));
    if (key != numerator_key && key != denominator_key) {
      continue;
    }

    uint64_t parsed = 0;
    if (!TryParseU64(value, &parsed)) {
      result.status = LoadStatus::kError;
      result.error = "Invalid integer for key '" + key + "' in " +
                     path.string();
      return result;
    }

    if (key == numerator_key) {
      numerator = parsed;
    } else {
      denominator = parsed;
    }
  }

  if (!numerator.has_value() && !denominator.has_value()) {
    result.status = LoadStatus::kMissingSection;
    return result;
  }
  if (!numerator.has_value() || !denominator.has_value()) {
    result.status = LoadStatus::kError;
    result.error = "Incomplete calibration section '" + section_name + "' in " +
                   path.string();
    return result;
  }
  if (*denominator == 0) {
    result.status = LoadStatus::kError;
    result.error = "Calibration denominator must be > 0 in " + path.string();
    return result;
  }

  result.status = LoadStatus::kLoaded;
  result.calibration = Calibration{*numerator, *denominator};
  return result;
}

}  // namespace burn_calibration
