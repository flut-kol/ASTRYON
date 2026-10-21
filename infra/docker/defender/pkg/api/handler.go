package api

import (
	"github.com/labstack/echo/v4"
	"go-defender/pkg/api/apierr"
	"go-defender/pkg/domain/dto"
	"net/http"
)

func Assemble(root *echo.Group, s Service) {
	h := &handler{service: s}

	root.GET("ping", h.ping)
	root.POST("scan", h.scanFile)
	root.POST("scan-files", h.scanFiles)
}

type handler struct {
	service Service
}

func (h *handler) ping(c echo.Context) error {
	ctx := c.Request().Context()
	res, err := h.service.Ping(ctx)
	if err != nil {
		return apierr.LogicError(err)
	}

	return c.JSON(http.StatusOK, res)
}

func (h *handler) scanFile(c echo.Context) error {
	ctx := c.Request().Context()
	binder := &echo.DefaultBinder{}

	var reqDto dto.FileScanRequest

	err := binder.BindBody(c, &reqDto)
	if err != nil {
		return apierr.BindingError(err)
	}

	res, err := h.service.ScanFile(ctx, reqDto)
	if err != nil {
		return apierr.LogicError(err)
	}

	return c.JSON(http.StatusOK, res)
}

func (h *handler) scanFiles(c echo.Context) error {
	ctx := c.Request().Context()
	binder := &echo.DefaultBinder{}

	var reqDto dto.FilesScanRequest

	err := binder.BindBody(c, &reqDto)
	if err != nil {
		return apierr.BindingError(err)
	}

	res, err := h.service.ScanFiles(ctx, reqDto)
	if err != nil {
		return apierr.LogicError(err)
	}

	return c.JSON(http.StatusOK, res)
}
