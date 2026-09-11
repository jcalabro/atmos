package host

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"

	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/api/comatproto"
	"github.com/jcalabro/atmos/space/credential"
	"github.com/jcalabro/atmos/space/notificationauth"
	"github.com/jcalabro/atmos/space/simplespace"
	"github.com/jcalabro/atmos/xrpc"
	"github.com/jcalabro/atmos/xrpcserver"
	"github.com/jcalabro/gt"
)

const (
	methodCreateSpace      = "com.atproto.simplespace.createSpace"
	methodUpdateSpace      = "com.atproto.simplespace.updateSpace"
	methodDeleteSpace      = "com.atproto.simplespace.deleteSpace"
	methodGetSpace         = "com.atproto.simplespace.getSpace"
	methodListMembers      = "com.atproto.simplespace.listMembers"
	methodPutMember        = "com.atproto.simplespace.putMember"
	methodRemoveMember     = "com.atproto.simplespace.removeMember"
	methodGetCredential    = "com.atproto.space.getSpaceCredential"
	methodListRepos        = "com.atproto.space.listRepos"
	methodNotifyWrite      = "com.atproto.space.notifyWrite"
	methodRegisterNotify   = "com.atproto.space.registerNotify"
	methodUnregisterNotify = "com.atproto.space.unregisterNotify"
)

// Mount registers all authority and simple-space management handlers. The
// embedding http.Server owns header/read/write/idle deadlines, trusted-proxy
// policy, listener lifecycle, and transport shutdown. Configure its maximum
// request size at or above Limits.MaxRequestBody; the host applies its own cap.
func (h *Host) Mount(server *xrpcserver.Server) error {
	if server == nil {
		return errors.New("space host: XRPC server is required")
	}
	server.HandleProcedure(methodCreateSpace, h.procedure(h.createSpace))
	server.HandleProcedure(methodUpdateSpace, h.procedure(h.updateSpace))
	server.HandleProcedure(methodDeleteSpace, h.procedure(h.deleteSpace))
	server.HandleQuery(methodGetSpace, xrpcserver.HandlerFunc(h.getSpace))
	server.HandleQuery(methodListMembers, xrpcserver.HandlerFunc(h.listMembers))
	server.HandleProcedure(methodPutMember, h.procedure(h.putMember))
	server.HandleProcedure(methodRemoveMember, h.procedure(h.removeMember))
	server.HandleProcedure(methodGetCredential, h.procedure(h.getSpaceCredential))
	server.HandleQuery(methodListRepos, xrpcserver.HandlerFunc(h.listRepos))
	server.HandleProcedure(methodNotifyWrite, h.procedure(h.notifyWrite))
	server.HandleProcedure(methodRegisterNotify, h.procedure(h.registerNotify))
	server.HandleProcedure(methodUnregisterNotify, h.procedure(h.unregisterNotify))
	return nil
}

type procedureFunc func(context.Context, http.ResponseWriter, *xrpcserver.Request) error

func (h *Host) procedure(fn procedureFunc) xrpcserver.Handler {
	return xrpcserver.HandlerFunc(func(ctx context.Context, w http.ResponseWriter, req *xrpcserver.Request) error {
		req.HTTPReq.Body = http.MaxBytesReader(w, req.HTTPReq.Body, h.limits.MaxRequestBody)
		return fn(ctx, w, req)
	})
}

func (h *Host) createSpace(ctx context.Context, w http.ResponseWriter, req *xrpcserver.Request) error {
	var input comatproto.SimplespaceCreateSpace_Input
	if err := decodeRequest(req, &input); err != nil {
		return err
	}
	typ, err := atmos.ParseNSID(input.Type)
	if err != nil {
		return invalidRequest("InvalidSpaceType", "invalid space type")
	}
	skey, err := generatedSpaceKey()
	if input.Skey.HasVal() {
		skey, err = atmos.ParseRecordKey(input.Skey.Val())
	}
	if err != nil {
		return invalidRequest("InvalidSpaceKey", "invalid space key")
	}
	principal, err := h.accountAuth.AuthenticateAccount(ctx, req.HTTPReq)
	if err != nil {
		return authError(err)
	}
	if err := principal.DID.Validate(); err != nil || principal.Permissions == nil {
		return authError(errors.New("invalid account principal"))
	}
	space, err := atmos.ParseSpaceRef("at://" + principal.DID.String() + "/space/" + typ.String() + "/" + skey.String())
	if err != nil {
		return invalidRequest("InvalidSpaceUri", "invalid derived space URI")
	}
	if !principal.Permissions.Allows(space, AccountCreate) {
		return xrpcserver.Forbidden("account grant does not permit space management")
	}
	config, err := simplespace.DecodeCreate(space, &input)
	if err != nil {
		return invalidRequest("InvalidPolicy", err.Error())
	}
	if _, err := h.store.CreateSpace(ctx, config, h.clock.Now()); err != nil {
		return storeError(err)
	}
	return writeJSON(w, &comatproto.SimplespaceCreateSpace_Output{URI: space.String()})
}

func (h *Host) updateSpace(ctx context.Context, w http.ResponseWriter, req *xrpcserver.Request) error {
	var input comatproto.SimplespaceUpdateSpace_Input
	if err := decodeRequest(req, &input); err != nil {
		return err
	}
	space, err := atmos.ParseSpaceRef(input.Space)
	if err != nil {
		return invalidRequest("InvalidSpaceUri", "invalid space URI")
	}
	if _, err := h.authenticateAccount(ctx, req.HTTPReq, space, AccountUpdate); err != nil {
		return authError(err)
	}
	patch, err := simplespace.DecodeUpdate(&input)
	if err != nil {
		return invalidRequest("InvalidPolicy", err.Error())
	}
	if _, err := h.store.UpdateSpace(ctx, space, patch, h.clock.Now()); err != nil {
		return storeError(err)
	}
	w.WriteHeader(http.StatusOK)
	return nil
}

func (h *Host) deleteSpace(ctx context.Context, w http.ResponseWriter, req *xrpcserver.Request) error {
	var input comatproto.SimplespaceDeleteSpace_Input
	if err := decodeRequest(req, &input); err != nil {
		return err
	}
	space, err := atmos.ParseSpaceRef(input.Space)
	if err != nil {
		return invalidRequest("InvalidSpaceUri", "invalid space URI")
	}
	if _, err := h.authenticateAccount(ctx, req.HTTPReq, space, AccountDelete); err != nil {
		return authError(err)
	}
	now := h.clock.Now()
	err = h.store.DeleteSpace(ctx, space, now, now.Add(h.limits.DeliveryRetention))
	if err != nil {
		return storeError(err)
	}
	h.emit(ctx, Event{Kind: EventSpaceDeleted, Space: space})
	w.WriteHeader(http.StatusOK)
	return nil
}

func (h *Host) getSpace(ctx context.Context, w http.ResponseWriter, req *xrpcserver.Request) error {
	space, err := requiredSpace(req, "space")
	if err != nil {
		return err
	}
	if _, err := h.authenticateAccount(ctx, req.HTTPReq, space, AccountReadSelf); err != nil {
		if !errors.Is(err, ErrNoAccountCredential) {
			return authError(err)
		}
		if _, err := h.authenticateReader(ctx, req.HTTPReq, space, methodGetSpace); err != nil {
			return authError(err)
		}
	}
	state, err := h.store.GetSpace(ctx, space)
	if err != nil || !state.Active() {
		return xrpcNotFound("SpaceNotFound", "space not found")
	}
	output, err := simplespace.Output(state.Config)
	if err != nil {
		return xrpcserver.InternalError("stored policy is invalid")
	}
	return writeJSON(w, output)
}

func (h *Host) listMembers(ctx context.Context, w http.ResponseWriter, req *xrpcserver.Request) error {
	space, err := requiredSpace(req, "space")
	if err != nil {
		return err
	}
	if _, err := h.authenticateAccount(ctx, req.HTTPReq, space, AccountReadSelf); err != nil {
		return authError(err)
	}
	limit, err := listLimit(req, h.limits.MaxListMembers)
	if err != nil {
		return err
	}
	cursor, err := optionalCursor(req)
	if err != nil {
		return err
	}
	after, err := decodeDIDCursor(cursor)
	if err != nil {
		return invalidRequest("InvalidCursor", "invalid cursor")
	}
	values, err := h.store.ListMembers(ctx, space, after, limit+1)
	if err != nil {
		return storeError(err)
	}
	out := &comatproto.SimplespaceListMembers_Output{Members: make([]comatproto.SimplespaceListMembers_Member, min(limit, len(values)))}
	for i := range out.Members {
		out.Members[i] = comatproto.SimplespaceListMembers_Member{DID: values[i].DID.String(), Read: values[i].Read, Write: values[i].Write}
	}
	if len(values) > limit {
		out.Cursor = gt.Some(encodeDIDCursor(values[limit-1].DID))
	}
	return writeJSON(w, out)
}

func (h *Host) putMember(ctx context.Context, w http.ResponseWriter, req *xrpcserver.Request) error {
	var input comatproto.SimplespacePutMember_Input
	if err := decodeRequest(req, &input); err != nil {
		return err
	}
	space, err := atmos.ParseSpaceRef(input.Space)
	if err != nil {
		return invalidRequest("InvalidSpaceUri", "invalid space URI")
	}
	if _, err := h.authenticateAccount(ctx, req.HTTPReq, space, AccountUpdate); err != nil {
		return authError(err)
	}
	did, err := atmos.ParseDID(input.DID)
	if err != nil {
		return invalidRequest("InvalidMember", "invalid member DID")
	}
	if err := h.store.PutMember(ctx, space, simplespace.Member{DID: did, Read: input.Read, Write: input.Write}, h.clock.Now()); err != nil {
		return storeError(err)
	}
	w.WriteHeader(http.StatusOK)
	return nil
}

func (h *Host) removeMember(ctx context.Context, w http.ResponseWriter, req *xrpcserver.Request) error {
	var input comatproto.SimplespaceRemoveMember_Input
	if err := decodeRequest(req, &input); err != nil {
		return err
	}
	space, err := atmos.ParseSpaceRef(input.Space)
	if err != nil {
		return invalidRequest("InvalidSpaceUri", "invalid space URI")
	}
	if _, err := h.authenticateAccount(ctx, req.HTTPReq, space, AccountUpdate); err != nil {
		return authError(err)
	}
	did, err := atmos.ParseDID(input.DID)
	if err != nil {
		return invalidRequest("InvalidMember", "invalid member DID")
	}
	if err := h.store.RemoveMember(ctx, space, did, h.clock.Now()); err != nil {
		return storeError(err)
	}
	w.WriteHeader(http.StatusOK)
	return nil
}

func (h *Host) getSpaceCredential(ctx context.Context, w http.ResponseWriter, req *xrpcserver.Request) error {
	var input comatproto.SpaceGetSpaceCredential_Input
	if err := decodeRequest(req, &input); err != nil {
		return err
	}
	space, err := atmos.ParseSpaceRef(input.Space)
	if err != nil {
		return invalidRequest("InvalidSpaceUri", "invalid space URI")
	}
	attestation := ""
	if input.ClientAttestation.HasVal() {
		attestation = input.ClientAttestation.Val()
	}
	principal, err := h.authenticateExchange(ctx, req.HTTPReq, space, attestation)
	if err != nil {
		if errors.Is(err, ErrInvalidClientAttestation) {
			return invalidRequest("InvalidClientAttestation", "client attestation verification failed")
		}
		return invalidRequest("InvalidDelegationToken", "credential exchange authentication failed")
	}
	state, err := h.store.GetSpace(ctx, space)
	if errors.Is(err, ErrTombstoned) || (err == nil && !state.Active()) {
		return invalidRequest("SpaceDeleted", "space has been deleted")
	}
	if err != nil {
		return xrpcNotFound("SpaceNotFound", "space not found")
	}
	if err := h.authorize(ctx, state, principal.User, simplespace.AccessRead, principal.ClientID); err != nil {
		if errors.Is(err, ErrAppDenied) {
			return invalidRequest("AppNotAuthorized", "application not authorized")
		}
		if errors.Is(err, ErrUserDenied) {
			return invalidRequest("UserNotAuthorized", "user not authorized")
		}
		return invalidRequest("NotAuthorized", "authorization dependency failed")
	}
	key, kid, err := h.signer.CredentialKey(ctx, space.Authority())
	if err != nil {
		return xrpcserver.InternalError("credential signer unavailable")
	}
	raw, err := credential.CreateSpaceCredentialToken(credential.SpaceCredentialTokenParams{
		Issuer: space.Authority(), Subject: space, DPoPThumbprint: principal.JKT,
		KeyID: kid, Now: h.clock.Now(), Lifetime: h.limits.CredentialLifetime,
	}, key)
	if err != nil {
		return xrpcserver.InternalError("credential signing failed")
	}
	current, err := h.store.GetSpace(ctx, space)
	if err != nil || !current.Active() || current.Generation != state.Generation {
		return invalidRequest("NotAuthorized", "space policy changed during authorization")
	}
	h.emit(ctx, Event{Kind: EventCredentialIssued, Space: space, DID: principal.User})
	return writeJSON(w, &comatproto.SpaceGetSpaceCredential_Output{Credential: raw})
}

func (h *Host) listRepos(ctx context.Context, w http.ResponseWriter, req *xrpcserver.Request) error {
	space, err := requiredSpace(req, "space")
	if err != nil {
		return err
	}
	if _, err := h.authenticateReader(ctx, req.HTTPReq, space, methodListRepos); err != nil {
		return authError(err)
	}
	limit, err := listLimit(req, h.limits.MaxListWriters)
	if err != nil {
		return err
	}
	cursor, err := optionalCursor(req)
	if err != nil {
		return err
	}
	after, err := decodeDIDCursor(cursor)
	if err != nil {
		return invalidRequest("InvalidCursor", "invalid cursor")
	}
	values, err := h.store.ListWriters(ctx, space, after, limit+1)
	if err != nil {
		return storeError(err)
	}
	out := &comatproto.SpaceListRepos_Output{Repos: make([]comatproto.SpaceListRepos_Repo, min(limit, len(values)))}
	for i := range out.Repos {
		out.Repos[i] = comatproto.SpaceListRepos_Repo{DID: values[i].DID.String(), Rev: values[i].Revision.String(), Hash: values[i].Hash[:]}
	}
	if len(values) > limit {
		out.Cursor = gt.Some(encodeDIDCursor(values[limit-1].DID))
	}
	return writeJSON(w, out)
}

func (h *Host) notifyWrite(ctx context.Context, w http.ResponseWriter, req *xrpcserver.Request) error {
	var input comatproto.SpaceNotifyWrite_Input
	if err := decodeRequest(req, &input); err != nil {
		return err
	}
	space, err := atmos.ParseSpaceRef(input.Space)
	if err != nil {
		return invalidRequest("InvalidSpaceUri", "invalid space URI")
	}
	repo, err := atmos.ParseDID(input.Repo)
	if err != nil {
		return invalidRequest("InvalidRepo", "invalid repo DID")
	}
	rev, err := atmos.ParseTID(input.Rev)
	if err != nil || len(input.Hash) != 32 {
		return invalidRequest("InvalidCommit", "invalid writer revision or hash")
	}
	raw, err := bearerToken(req.HTTPReq)
	if err != nil {
		return authError(err)
	}
	if _, err := notificationauth.VerifyWriterNotifyWrite(ctx, raw, space, repo, notificationauth.WriterOptions{
		Options: notificationauth.Options{Resolver: h.resolver, Replay: h.replay}, Authority: space.Authority(),
	}); err != nil {
		return authError(err)
	}
	state, err := h.store.GetSpace(ctx, space)
	if err != nil || !state.Active() {
		return xrpcNotFound("SpaceNotFound", "space not found")
	}
	if err := h.authorize(ctx, state, repo, simplespace.AccessWrite, ""); err != nil {
		if errors.Is(err, ErrDenied) {
			return xrpcserver.Forbidden("writer is not authorized")
		}
		return xrpcserver.Forbidden("write policy evaluation failed")
	}
	var hash [32]byte
	copy(hash[:], input.Hash)
	result, err := h.store.AdmitWriter(ctx, space, state.Generation, Writer{DID: repo, Revision: rev, Hash: hash}, h.clock.Now())
	if err != nil {
		return storeError(err)
	}
	if result == AdmissionAdvanced {
		h.emit(ctx, Event{Kind: EventWriterAccepted, Space: space, DID: repo})
	}
	w.WriteHeader(http.StatusOK)
	return nil
}

func (h *Host) registerNotify(ctx context.Context, w http.ResponseWriter, req *xrpcserver.Request) error {
	var input comatproto.SpaceRegisterNotify_Input
	if err := decodeRequest(req, &input); err != nil {
		return err
	}
	space, err := atmos.ParseSpaceRef(input.Space)
	if err != nil {
		return invalidRequest("InvalidSpaceUri", "invalid space URI")
	}
	principal, err := h.authenticateReader(ctx, req.HTTPReq, space, methodRegisterNotify)
	if err != nil {
		return authError(err)
	}
	if err := simplespace.ValidateServiceIdentifier(input.Service); err != nil {
		return invalidRequest("ServiceNotResolvable", "invalid service identifier")
	}
	decision, err := h.subscribers.AuthorizeSubscriber(ctx, SubscriberRequest{
		Operation: SubscriberRegister, Space: space, Service: input.Service, CredentialID: principal.CredentialID,
	})
	if err != nil || !decision.Allowed || decision.ServiceType == "" {
		return xrpcserver.Forbidden("subscriber registration denied")
	}
	if _, err := h.resolveService(ctx, input.Service, decision.ServiceType); err != nil {
		return invalidRequest("ServiceNotResolvable", "service could not be strictly resolved")
	}
	now := h.clock.Now()
	expires := now.Add(h.limits.RegistrationTTL)
	if err := h.store.Register(ctx, Registration{
		Space: space, Service: input.Service, ServiceType: decision.ServiceType,
		CredentialID: principal.CredentialID, ExpiresAt: expires, UpdatedAt: now,
	}, h.limits.Registration); err != nil {
		return storeError(err)
	}
	return writeJSON(w, &comatproto.SpaceRegisterNotify_Output{ExpiresAt: expires.UTC().Format(atmos.AtprotoDatetimeLayout)})
}

func (h *Host) unregisterNotify(ctx context.Context, w http.ResponseWriter, req *xrpcserver.Request) error {
	var input comatproto.SpaceUnregisterNotify_Input
	if err := decodeRequest(req, &input); err != nil {
		return err
	}
	space, err := atmos.ParseSpaceRef(input.Space)
	if err != nil {
		return invalidRequest("InvalidSpaceUri", "invalid space URI")
	}
	principal, err := h.authenticateReader(ctx, req.HTTPReq, space, methodUnregisterNotify)
	if err != nil {
		return authError(err)
	}
	if err := simplespace.ValidateServiceIdentifier(input.Service); err != nil {
		return invalidRequest("ServiceNotResolvable", "invalid service identifier")
	}
	decision, err := h.subscribers.AuthorizeSubscriber(ctx, SubscriberRequest{
		Operation: SubscriberWithdraw, Space: space, Service: input.Service, CredentialID: principal.CredentialID,
	})
	if err != nil || !decision.Allowed {
		return xrpcserver.Forbidden("subscriber withdrawal denied")
	}
	if err := h.store.Unregister(ctx, space, input.Service, h.clock.Now()); err != nil {
		return storeError(err)
	}
	w.WriteHeader(http.StatusOK)
	return nil
}

func decodeRequest(req *xrpcserver.Request, output any) error {
	decoder := json.NewDecoder(req.HTTPReq.Body)
	if err := decoder.Decode(output); err != nil {
		if _, ok := errors.AsType[*http.MaxBytesError](err); ok {
			return xrpcserver.TooLarge("request body too large")
		}
		return xrpcserver.InvalidRequest("invalid request body")
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return xrpcserver.InvalidRequest("request body must contain exactly one JSON value")
	}
	return nil
}

func writeJSON(w http.ResponseWriter, value any) error {
	w.Header().Set("Content-Type", "application/json")
	encoder := json.NewEncoder(w)
	if err := encoder.Encode(value); err != nil {
		return xrpcserver.InternalError("failed to encode response")
	}
	return nil
}

func requiredSpace(req *xrpcserver.Request, key string) (atmos.SpaceRef, error) {
	values := req.Params.Strings(key)
	if len(values) != 1 {
		return "", xrpcserver.InvalidRequest("exactly one " + key + " parameter is required")
	}
	space, err := atmos.ParseSpaceRef(values[0])
	if err != nil {
		return "", invalidRequest("InvalidSpaceUri", "invalid space URI")
	}
	return space, nil
}

func listLimit(req *xrpcserver.Request, maximum int) (int, error) {
	values := req.Params.Strings("limit")
	if len(values) == 0 {
		return min(50, maximum), nil
	}
	if len(values) != 1 {
		return 0, xrpcserver.InvalidRequest("limit must appear once")
	}
	value, err := strconv.Atoi(values[0])
	if err != nil || value <= 0 || value > maximum {
		return 0, xrpcserver.InvalidRequest("limit is outside the configured bound")
	}
	return value, nil
}

func optionalCursor(req *xrpcserver.Request) (string, error) {
	values := req.Params.Strings("cursor")
	if len(values) == 0 {
		return "", nil
	}
	if len(values) != 1 || values[0] == "" {
		return "", xrpcserver.InvalidRequest("cursor must be one nonempty value")
	}
	return values[0], nil
}

func encodeDIDCursor(did atmos.DID) string {
	return base64.RawURLEncoding.EncodeToString([]byte(did))
}

func decodeDIDCursor(raw string) (atmos.DID, error) {
	if raw == "" {
		return "", nil
	}
	decoded, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil || len(decoded) > 2048 {
		return "", errors.New("invalid cursor")
	}
	return atmos.ParseDID(string(decoded))
}

func invalidRequest(name, message string) *xrpc.Error {
	return &xrpc.Error{StatusCode: http.StatusBadRequest, Name: name, Message: message}
}

func xrpcNotFound(name, message string) *xrpc.Error {
	return &xrpc.Error{StatusCode: http.StatusNotFound, Name: name, Message: message}
}

func authError(err error) *xrpc.Error {
	if errors.Is(err, ErrDenied) {
		return xrpcserver.Forbidden("not authorized")
	}
	return xrpcserver.AuthRequired("valid operation-specific authentication is required")
}

func storeError(err error) *xrpc.Error {
	switch {
	case errors.Is(err, ErrNotFound), errors.Is(err, ErrTombstoned):
		return xrpcNotFound("SpaceNotFound", "space not found")
	case errors.Is(err, ErrAlreadyExists):
		return invalidRequest("SpaceAlreadyExists", "space already exists")
	case errors.Is(err, ErrStaleRevision):
		return invalidRequest("StaleRevision", "writer revision is stale")
	case errors.Is(err, ErrRevisionConflict):
		return invalidRequest("RevisionConflict", "writer revision conflicts with durable state")
	case errors.Is(err, ErrPolicyChanged):
		return xrpcserver.Forbidden("space policy changed during authorization")
	case errors.Is(err, ErrQuota), errors.Is(err, ErrOutboxFull):
		return xrpcserver.RateLimited("authority capacity exhausted")
	default:
		return xrpcserver.InternalError("authority store operation failed")
	}
}
