from dataclasses import dataclass
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from .subject import VerifyResult


@dataclass
class Envelope:
    """
    MOSS Envelope - cryptographically signed agent output.
    
    Contains the signature, metadata, and can self-verify.
    """
    spec: str
    version: int
    alg: str
    subject: str
    key_version: int
    seq: int
    issued_at: int
    payload_hash: str
    signature: str

    def to_dict(self) -> dict:
        return {
            "spec": self.spec,
            "version": self.version,
            "alg": self.alg,
            "subject": self.subject,
            "key_version": self.key_version,
            "seq": self.seq,
            "issued_at": self.issued_at,
            "payload_hash": self.payload_hash,
            "signature": self.signature
        }

    @classmethod
    def from_dict(cls, d: dict) -> "Envelope":
        return cls(
            spec=d["spec"],
            version=d["version"],
            alg=d["alg"],
            subject=d["subject"],
            key_version=d["key_version"],
            seq=d["seq"],
            issued_at=d["issued_at"],
            payload_hash=d["payload_hash"],
            signature=d["signature"]
        )

    def verify(
        self,
        payload: Any = None,
        check_replay: bool = False,
    ) -> "VerifyResult":
        """
        Verify this envelope's signature.
        
        Args:
            payload: Original payload for hash verification (optional)
            check_replay: Whether to check sequence numbers (default: False)
        
        Returns:
            VerifyResult with valid=True/False and details
        
        Example:
            envelope = agent.sign({"action": "transfer", "amount": 50000})
            result = envelope.verify()
            if result.valid:
                print(f"Signed by: {result.subject}")
        """
        from .subject import Subject
        return Subject.verify(
            envelope=self,
            payload=payload,
            check_replay=check_replay,
        )
    
    @property
    def agent_id(self) -> str:
        """Alias for subject - the agent that signed this envelope."""
        return self.subject
    
    @property
    def timestamp(self) -> int:
        """Unix timestamp when this envelope was signed."""
        return self.issued_at
