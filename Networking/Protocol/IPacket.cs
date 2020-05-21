using System;

namespace WFast.Networking.Protocol
{
    public interface IPacket
    {
        public ReadOnlySpan<byte> GetByteSpan();
    }
}
