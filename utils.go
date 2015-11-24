package ali_mns

import (
	"net/http"

	"github.com/gogap/errors"
)

func send(client MNSClient, decoder MNSDecoder, method Method, headers map[string]string, message interface{}, resource string, v interface{}) (statusCode int, err error) {
	var resp *http.Response
	if resp, err = client.Send(method, headers, message, resource); err != nil {
		return
	}

	if resp != nil {
		defer resp.Body.Close()
		statusCode = resp.StatusCode

		if resp.StatusCode != http.StatusCreated &&
			resp.StatusCode != http.StatusOK &&
			resp.StatusCode != http.StatusNoContent {

			errResp := ErrorMessageResponse{}
			if e := decoder.Decode(resp.Body, &errResp); e != nil {
				err = ERR_UNMARSHAL_ERROR_RESPONSE_FAILED.New(errors.Params{"err": e})
				return
			}
			err = ParseError(errResp, resource)
			return
		}

		if v != nil {
			if e := decoder.Decode(resp.Body, v); e != nil {
				err = ERR_UNMARSHAL_RESPONSE_FAILED.New(errors.Params{"err": e})
				return
			}
		}
	}

	return
}
