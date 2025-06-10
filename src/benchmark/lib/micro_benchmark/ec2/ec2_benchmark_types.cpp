#include "ec2_benchmark_types.hpp"

#include "utils/assert.hpp"
#include "utils/unit_conversion.hpp"

namespace skyrise {

size_t Ec2InstanceMemorySizeMiB(Ec2InstanceType instance_type) {
  switch (instance_type) {
    case Ec2InstanceType::kT3Nano:
    case Ec2InstanceType::kT4GNano:
      return 512;
    case Ec2InstanceType::kT3Micro:
    case Ec2InstanceType::kT4GMicro:
      return GiBToMiB(1);
    case Ec2InstanceType::kT3Small:
    case Ec2InstanceType::kT4GSmall:
    case Ec2InstanceType::kC6GMedium:
    case Ec2InstanceType::kC6GNMedium:
      return GiBToMiB(2);
    case Ec2InstanceType::kT3Medium:
    case Ec2InstanceType::kT4GMedium:
    case Ec2InstanceType::kC5NLarge:
    case Ec2InstanceType::kM6GMedium:
    case Ec2InstanceType::kC6GLarge:
    case Ec2InstanceType::kC6GNLarge:
      return GiBToMiB(4);
    case Ec2InstanceType::kT3Large:
    case Ec2InstanceType::kT4GLarge:
    case Ec2InstanceType::kM6ILarge:
    case Ec2InstanceType::kM6GLarge:
    case Ec2InstanceType::kC6GXLarge:
    case Ec2InstanceType::kC6GNXLarge:
      return GiBToMiB(8);
    case Ec2InstanceType::kT3XLarge:
    case Ec2InstanceType::kT4GXLarge:
    case Ec2InstanceType::kM6IXLarge:
    case Ec2InstanceType::kM6GXLarge:
    case Ec2InstanceType::kC6G2XLarge:
    case Ec2InstanceType::kC6GN2XLarge:
    case Ec2InstanceType::kC7GN2XLarge:
      return GiBToMiB(16);
    case Ec2InstanceType::kT22XLarge:
    case Ec2InstanceType::kT32XLarge:
    case Ec2InstanceType::kT4G2XLarge:
    case Ec2InstanceType::kM6I2XLarge:
    case Ec2InstanceType::kM6G2XLarge:
    case Ec2InstanceType::kC6G4XLarge:
    case Ec2InstanceType::kC6GN4XLarge:
      return GiBToMiB(32);
    case Ec2InstanceType::kM6I4XLarge:
    case Ec2InstanceType::kM6G4XLarge:
    case Ec2InstanceType::kC6G8XLarge:
    case Ec2InstanceType::kC6GN8XLarge:
      return GiBToMiB(64);
    case Ec2InstanceType::kC6G12XLarge:
    case Ec2InstanceType::kC6GN12XLarge:
      return GiBToMiB(96);
    case Ec2InstanceType::kM5N8XLarge:
    case Ec2InstanceType::kM6I8XLarge:
    case Ec2InstanceType::kM6G8XLarge:
    case Ec2InstanceType::kC6G16XLarge:
    case Ec2InstanceType::kC6GN16XLarge:
      return GiBToMiB(128);
    case Ec2InstanceType::kM6I12XLarge:
    case Ec2InstanceType::kM6G12XLarge:
      return GiBToMiB(192);
    case Ec2InstanceType::kM416XLarge:
    case Ec2InstanceType::kM516XLarge:
    case Ec2InstanceType::kM6I16XLarge:
    case Ec2InstanceType::kM6G16XLarge:
    case Ec2InstanceType::kM6GMetal:
      return GiBToMiB(256);
    case Ec2InstanceType::kM6I24XLarge:
      return GiBToMiB(384);
    case Ec2InstanceType::kM6I32XLarge:
    case Ec2InstanceType::kM6IMetal:
      return GiBToMiB(512);
    default:
      Fail("Unsupported EC2 instance type.");
  }
}

}  // namespace skyrise
