package org.simulation;

import org.simulation.proto.generated.SimulationPackets;
import org.transport.service.impl.PacketDefinition;

public class Packets {

    public static final PacketDefinition<
                SimulationPackets.TranslationRequest
                > TRANSLATION_REQUEST =
            new PacketDefinition<>(
                    2,
                    SimulationPackets.TranslationRequest.parser()
            );

    public static final PacketDefinition<
            SimulationPackets.TranslationResult
            > TRANSLATION_RESULT =
            new PacketDefinition<>(
                    3,
                    SimulationPackets.TranslationResult.parser()
            );

}
