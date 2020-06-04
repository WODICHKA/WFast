using System;

namespace WFast.Networking.Protocol
{
    public unsafe delegate
              int PacketCalculateEvent(byte* buffer);
    public interface IPacket
    {
        public ReadOnlySpan<byte> GetByteSpan();
    }
}
