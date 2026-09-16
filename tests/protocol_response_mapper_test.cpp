#include "snf/server/protocol_response_mapper.hpp"
#include "snf/game/room_result.hpp"

#include <cassert>

namespace
{
    static_assert(static_cast<std::uint16_t>(snf::protocol::MessageType::Purchase) == 11);
    static_assert(static_cast<std::uint16_t>(snf::protocol::MessageType::PurchaseResult) == 12);
    static_assert(static_cast<std::uint16_t>(snf::protocol::MessageType::MoveAcknowledged) == 31);
    static_assert(static_cast<std::uint16_t>(snf::protocol::MessageType::EquipSkill) == 32);
    static_assert(static_cast<std::uint16_t>(snf::protocol::MessageType::EquipSkillResult) == 33);
    static_assert(static_cast<std::uint8_t>(snf::server::PurchaseStatus::AlreadyOwned) == 7);
    static_assert(static_cast<std::uint8_t>(snf::server::RoomCommandStatus::SkillNotEquipped) == 13);

    void test_maps_typed_pong_response_to_wire_frame()
    {
        const snf::server::ProtocolResponseMapper mapper;
        const auto frame = mapper.map(
            snf::server::PongResponse{
                .payload = {std::byte{0xAB}},
            },
            19);

        assert(frame.type == snf::protocol::MessageType::Pong);
        assert(frame.request_id == 19);
        assert(frame.payload == std::vector<std::byte>{std::byte{0xAB}});
    }

    void test_maps_authenticated_response_with_persistent_player_id()
    {
        const snf::server::ProtocolResponseMapper mapper;
        const auto frame = mapper.map(
            snf::server::AuthenticatedResponse{
                .player = snf::server::PlayerId{.value = 0x0102030405060708ULL},
            },
            20);

        assert(frame.type == snf::protocol::MessageType::Authenticated);
        assert(frame.request_id == 20);
        assert(frame.payload == std::vector<std::byte>({
                                    std::byte{0x01},
                                    std::byte{0x02},
                                    std::byte{0x03},
                                    std::byte{0x04},
                                    std::byte{0x05},
                                    std::byte{0x06},
                                    std::byte{0x07},
                                    std::byte{0x08},
                                }));
    }

    void test_maps_purchase_result_with_stable_wire_layout()
    {
        const snf::server::ProtocolResponseMapper mapper;
        const auto frame = mapper.map(
            snf::server::PurchaseResponse{
                .result =
                    snf::server::PurchaseTransactionResult{
                        .status = snf::server::PurchaseStatus::IdempotencyConflict,
                        .player = snf::server::PlayerId{.value = 9},
                        .idempotency_key = snf::server::PurchaseIdempotencyKey{.value = 0x0102030405060708ULL},
                        .product = snf::server::ProductId{.value = 0x11223344U},
                        .currency_balance = 0x1011121314151617ULL,
                        .purchased_item_count = 0x2021222324252627ULL,
                        .replayed = true,
                    },
            },
            21);

        assert(frame.type == snf::protocol::MessageType::PurchaseResult);
        assert(frame.request_id == 21);
        assert(frame.payload ==
               std::vector<std::byte>({
                   std::byte{0x04}, std::byte{0x01}, std::byte{0x01}, std::byte{0x02}, std::byte{0x03}, std::byte{0x04}, std::byte{0x05}, std::byte{0x06}, std::byte{0x07}, std::byte{0x08},
                   std::byte{0x11}, std::byte{0x22}, std::byte{0x33}, std::byte{0x44}, std::byte{0x10}, std::byte{0x11}, std::byte{0x12}, std::byte{0x13}, std::byte{0x14}, std::byte{0x15},
                   std::byte{0x16}, std::byte{0x17}, std::byte{0x20}, std::byte{0x21}, std::byte{0x22}, std::byte{0x23}, std::byte{0x24}, std::byte{0x25}, std::byte{0x26}, std::byte{0x27},
               }));
    }

    void test_maps_equip_skill_result_with_stable_wire_layout()
    {
        const snf::server::ProtocolResponseMapper mapper;
        const auto frame = mapper.map(
            snf::server::EquipSkillResponse{
                .status = snf::server::EquipSkillStatus::Equipped,
                .equipped_skill_id = snf::server::SkillId{.value = 0x11223344U},
            },
            22
        );

        assert(frame.type == snf::protocol::MessageType::EquipSkillResult);
        assert(frame.request_id == 22);
        assert(frame.payload == std::vector<std::byte>({
                                    std::byte{0x00}, std::byte{0x11}, std::byte{0x22}, std::byte{0x33}, std::byte{0x44},
                                }));
    }
}

void run_protocol_response_mapper_tests()
{
    test_maps_typed_pong_response_to_wire_frame();
    test_maps_authenticated_response_with_persistent_player_id();
    test_maps_purchase_result_with_stable_wire_layout();
    test_maps_equip_skill_result_with_stable_wire_layout();
}
