package apierr

import (
	"fmt"
	"github.com/labstack/echo/v4"
	"net/http"
)

func BindingError(err error) *echo.HTTPError {
	return echo.NewHTTPError(http.StatusBadRequest, fmt.Sprintf("invalid json: %v", err))
}

func LogicError(err error) *echo.HTTPError {
	return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
}
