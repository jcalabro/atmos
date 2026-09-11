package host

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/space/credential"
	"github.com/jcalabro/atmos/space/simplespace"
)

type readerPrincipal struct {
	CredentialID string
	JKT          string
}

type exchangePrincipal struct {
	User     atmos.DID
	JKT      string
	ClientID string
}

func (h *Host) authenticateAccount(ctx context.Context, req *http.Request, space atmos.SpaceRef, action AccountAction) (AccountPrincipal, error) {
	principal, err := h.accountAuth.AuthenticateAccount(ctx, req)
	if err != nil {
		return AccountPrincipal{}, err
	}
	if err := principal.DID.Validate(); err != nil || principal.Permissions == nil {
		return AccountPrincipal{}, errors.New("space host: account authenticator returned an invalid principal")
	}
	if principal.DID != space.Authority() {
		return AccountPrincipal{}, ErrDenied
	}
	if !principal.Permissions.Allows(space, action) {
		return AccountPrincipal{}, ErrDenied
	}
	return principal, nil
}

func (h *Host) authenticateReader(ctx context.Context, req *http.Request, space atmos.SpaceRef, method string) (readerPrincipal, error) {
	raw, err := authorizationToken(req, "DPoP")
	if err != nil {
		return readerPrincipal{}, err
	}
	token, err := credential.ParseSpaceCredentialToken(raw)
	if err != nil {
		return readerPrincipal{}, fmt.Errorf("space host: parse space credential: %w", err)
	}
	verified, err := credential.VerifySpaceCredentialToken(ctx, token, credential.VerifySpaceCredentialOptions{
		Resolver: h.resolver, Subject: space, Now: h.clock.Now(),
	})
	if err != nil {
		return readerPrincipal{}, fmt.Errorf("space host: verify space credential: %w", err)
	}
	proof, err := dpopHeader(req)
	if err != nil {
		return readerPrincipal{}, err
	}
	if _, err := credential.VerifyDPoPProof(ctx, proof, credential.VerifyDPoPOptions{
		Method: req.Method, TargetURL: h.endpoint(method),
		Credential: raw, ExpectedJKT: verified.ConfirmationJKT, Now: h.clock.Now(), Replay: h.replay,
	}); err != nil {
		return readerPrincipal{}, fmt.Errorf("space host: verify DPoP proof: %w", err)
	}
	return readerPrincipal{CredentialID: verified.JTI, JKT: verified.ConfirmationJKT}, nil
}

func (h *Host) authenticateExchange(ctx context.Context, req *http.Request, space atmos.SpaceRef, attestation string) (exchangePrincipal, error) {
	raw, err := bearerToken(req)
	if err != nil {
		return exchangePrincipal{}, errors.Join(ErrInvalidDelegation, err)
	}
	token, err := credential.ParseDelegationToken(raw)
	if err != nil {
		return exchangePrincipal{}, errors.Join(ErrInvalidDelegation, fmt.Errorf("space host: parse delegation: %w", err))
	}
	issuer, err := atmos.ParseDID(token.Issuer)
	if err != nil {
		return exchangePrincipal{}, errors.Join(ErrInvalidDelegation, fmt.Errorf("space host: invalid delegation issuer: %w", err))
	}
	if _, err := credential.VerifyDelegationToken(ctx, token, credential.VerifyDelegationOptions{
		Resolver: h.resolver, Issuer: issuer, Subject: space,
		Audience: credential.SpaceHostAudience(space.Authority()), Now: h.clock.Now(), Replay: h.replay,
	}); err != nil {
		return exchangePrincipal{}, errors.Join(ErrInvalidDelegation, fmt.Errorf("space host: verify delegation: %w", err))
	}
	proof, err := dpopHeader(req)
	if err != nil {
		return exchangePrincipal{}, errors.Join(ErrInvalidDelegation, err)
	}
	verifiedProof, err := credential.VerifyDPoPProof(ctx, proof, credential.VerifyDPoPOptions{
		Method: req.Method, TargetURL: h.endpoint("com.atproto.space.getSpaceCredential"),
		Now: h.clock.Now(), Replay: h.replay,
	})
	if err != nil {
		return exchangePrincipal{}, errors.Join(ErrInvalidDelegation, fmt.Errorf("space host: verify exchange DPoP: %w", err))
	}
	clientID := ""
	if attestation != "" {
		clientID, err = h.attestations.VerifyAttestation(ctx, attestation, credential.SpaceHostAudience(space.Authority()), h.clock.Now(), h.replay)
		if err != nil {
			return exchangePrincipal{}, errors.Join(ErrInvalidClientAttestation, fmt.Errorf("space host: verify client attestation: %w", err))
		}
	}
	return exchangePrincipal{User: issuer, JKT: verifiedProof.JKT, ClientID: clientID}, nil
}

func (h *Host) authorize(ctx context.Context, state SpaceState, user atmos.DID, access simplespace.Access, clientID string) error {
	if !state.Active() {
		return ErrTombstoned
	}
	if access == simplespace.AccessRead {
		switch state.Config.AppAccess.Kind {
		case simplespace.AppAccessOpen:
		case simplespace.AppAccessAllowList:
			allowed := false
			for _, value := range state.Config.AppAccess.Allowed {
				if value == clientID {
					allowed = true
					break
				}
			}
			if !allowed {
				return ErrAppDenied
			}
		default:
			return errors.New("space host: unsupported app-access policy")
		}
	}
	var member *simplespace.Member
	policy := state.Config.ReadPolicy
	if access == simplespace.AccessWrite {
		policy = state.Config.WritePolicy
	}
	if policy.Kind == simplespace.PolicyMemberList {
		value, err := h.store.GetMember(ctx, state.Config.URI, user)
		if err == nil {
			member = &value
		} else if !errors.Is(err, ErrNotFound) {
			return err
		}
	}
	allowed, err := h.policies.Authorize(ctx, PolicyRequest{
		Config: state.Config.Clone(), Member: member, User: user, Access: access, ClientID: clientID,
	})
	if err != nil {
		return err
	}
	if !allowed {
		return ErrUserDenied
	}
	return nil
}

func bearerToken(req *http.Request) (string, error) {
	return authorizationToken(req, "Bearer")
}

func authorizationToken(req *http.Request, scheme string) (string, error) {
	values := req.Header.Values("Authorization")
	if len(values) != 1 {
		return "", errors.New("space host: exactly one Authorization header is required")
	}
	parts := strings.Fields(values[0])
	if len(parts) != 2 || !strings.EqualFold(parts[0], scheme) || parts[1] == "" {
		return "", fmt.Errorf("space host: %s authorization is required", scheme)
	}
	return parts[1], nil
}

func dpopHeader(req *http.Request) (string, error) {
	values := req.Header.Values("DPoP")
	if len(values) != 1 || values[0] == "" || strings.ContainsAny(values[0], " \t\r\n,") {
		return "", errors.New("space host: exactly one valid DPoP header is required")
	}
	return values[0], nil
}
