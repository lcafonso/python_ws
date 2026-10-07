"""
Conversão dos pontos da IA (píxeis da câmara de intervenção) em coordenadas do braço
(secção 13 do relatório):

    P_camera = Z * K^-1 * [u, v, 1]          (modelo pinhole, distância Z ao plano da planta)
    P_robot  = T_robot_camera × P_camera

Para o teste inicial usa uma calibração simples num ficheiro JSON (ver calibracao_exemplo.json):
  - intrínsecos da câmara (fx, fy, cx, cy) para a resolução indicada;
  - distância da câmara ao plano da planta (Z, em metros), assumida constante;
  - matriz 4x4 T_robot_camera (pose da câmara no referencial do braço).
Se o ponto já trouxer 'position' (3D no frame da câmara), usa-a diretamente.

Os valores do ficheiro de exemplo NÃO são uma calibração real: servem apenas para testar a cadeia.
"""
from __future__ import annotations

import json
from dataclasses import dataclass

from .messages import AIResponse, utc_now


@dataclass
class Calibration:
    fx: float
    fy: float
    cx: float
    cy: float
    width: int
    height: int
    plane_distance_m: float
    T_robot_camera: list         # 4x4
    camera_frame: str = "intervention_camera"
    robot_frame: str = "robot_b_base"
    description: str = ""

    @staticmethod
    def load(path: str) -> "Calibration":
        with open(path, encoding="utf-8") as f:
            d = json.load(f)
        cam = d["camera"]
        T = d["T_robot_camera"]
        if len(T) != 4 or any(len(r) != 4 for r in T):
            raise ValueError("T_robot_camera tem de ser uma matriz 4x4")
        return Calibration(cam["fx"], cam["fy"], cam["cx"], cam["cy"], cam["width"], cam["height"],
                           d["plane_distance_m"], T, d.get("camera_frame", "intervention_camera"),
                           d.get("robot_frame", "robot_b_base"), d.get("description", ""))

    def pixel_to_camera(self, u, v, img_w=None, img_h=None):
        # se a imagem tiver outra resolução, escala os intrínsecos
        sx = (img_w / self.width) if img_w else 1.0
        sy = (img_h / self.height) if img_h else 1.0
        fx, fy, cx, cy = self.fx * sx, self.fy * sy, self.cx * sx, self.cy * sy
        z = self.plane_distance_m
        return ((u - cx) / fx * z, (v - cy) / fy * z, z)

    def camera_to_robot(self, p):
        T = self.T_robot_camera
        x, y, z = p
        return tuple(T[i][0] * x + T[i][1] * y + T[i][2] * z + T[i][3] for i in range(3))


def response_to_robot(resp: AIResponse, cal: Calibration) -> dict:
    if resp.frame_id != cal.camera_frame:
        raise ValueError(f"frame_id '{resp.frame_id}' não corresponde à calibração ('{cal.camera_frame}')")
    pts = []
    for p in resp.points:
        if p.position is not None:
            pc = (p.position.x, p.position.y, p.position.z)
            origem = "position"
        else:
            pc = cal.pixel_to_camera(p.pixel.u, p.pixel.v, resp.image_width, resp.image_height)
            origem = "pixel"
        pr = cal.camera_to_robot(pc)
        pts.append({
            "id": p.id,
            "type": p.type,
            "confidence": p.confidence,
            "position": {"x": round(pr[0], 4), "y": round(pr[1], 4), "z": round(pr[2], 4)},
            "source": origem,
            "source_pixel": {"u": p.pixel.u, "v": p.pixel.v} if p.pixel else None,
            "camera_position": {"x": round(pc[0], 4), "y": round(pc[1], 4), "z": round(pc[2], 4)},
        })
    return {
        "object_id": resp.object_id,
        "image_id": resp.image_id,
        "analysis_id": resp.analysis_id,
        "request_id": resp.request_id,
        "intervention_type": resp.intervention_type,
        "frame_id": cal.robot_frame,
        "units": "m",
        "calibration": cal.description,
        "points": pts,
        "timestamp": utc_now(),
    }
